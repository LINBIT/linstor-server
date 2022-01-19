package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.db.utils.K8sCrdUtils;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.WritecacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.LayerWritecacheVolumesSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class WritecacheLayerK8sCrdDriver implements WritecacheLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    private final HashMap<Integer, HashMap<Integer, GenCrdCurrent.LayerWritecacheVolumesSpec>> vlmSpecCache;

    @Inject
    public WritecacheLayerK8sCrdDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        vlmSpecCache = new HashMap<>();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public void fetchForLoadAll()
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, GenCrdCurrent.LayerWritecacheVolumesSpec> writecacheVlmSpecMap = tx.getSpec(
            GeneratedDatabaseTables.LAYER_WRITECACHE_VOLUMES
        );
        for (GenCrdCurrent.LayerWritecacheVolumesSpec bcacheVlmSpec : writecacheVlmSpecMap.values())
        {
            HashMap<Integer, GenCrdCurrent.LayerWritecacheVolumesSpec> map = vlmSpecCache
                .get(bcacheVlmSpec.layerResourceId);
            if (map == null)
            {
                map = new HashMap<>();
                vlmSpecCache.put(bcacheVlmSpec.layerResourceId, map);
            }
            map.put(bcacheVlmSpec.vlmNr, bcacheVlmSpec);
        }
    }

    @Override
    public void clearLoadAllCache()
    {
        vlmSpecCache.clear();
    }

    /**
     * Fully loads a {@link WritecacheRscData} object including its {@link WritecacheVlmData}
     *
     * @param parentRef
     * @return a {@link Pair}, where the first object is the actual WritecacheRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     * @throws DatabaseException
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<WritecacheRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException
    {
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, WritecacheVlmData<RSC>> vlmMap = new TreeMap<>();
        WritecacheRscData<RSC> writecacheRscData = new WritecacheRscData<>(
            id,
            absRsc,
            parentRef,
            children,
            rscSuffixRef,
            this,
            vlmMap,
            transObjFactory,
            transMgrProvider
        );

        int vlmNrInt = -1;


        try
        {
            Map<Integer, LayerWritecacheVolumesSpec> vlmSpecsMap = K8sCrdUtils.getCheckedVlmMap(
                dbCtx,
                absRsc,
                vlmSpecCache,
                id
            );

            for (Entry<Integer, GenCrdCurrent.LayerWritecacheVolumesSpec> entry : vlmSpecsMap.entrySet())
            {
                vlmNrInt = entry.getKey();
                LayerWritecacheVolumesSpec writecacheVlmSpec = entry.getValue();
                String cacheStorPoolNameStr = writecacheVlmSpec.poolName;

                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);
                StorPool cacheStorPool = null;
                if (cacheStorPoolNameStr != null)
                {
                    cacheStorPool = tmpStorPoolMapRef.get(
                        new Pair<>(
                            new NodeName(writecacheVlmSpec.nodeName),
                            new StorPoolName(cacheStorPoolNameStr)
                        )
                    ).objA;
                }

                vlmMap.put(
                    vlm.getVolumeNumber(),
                    new WritecacheVlmData<>(
                        vlm,
                        writecacheRscData,
                        cacheStorPool,
                        this,
                        transObjFactory,
                        transMgrProvider
                    )
                );
            }
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored volume number " + vlmNrInt +
                    " for resource layer id: " + writecacheRscData.getRscLayerId()
            );
        }
        catch (InvalidNameException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored storage pool name '" + exc.invalidName +
                    "' for resource layer id " + writecacheRscData.getRscLayerId() + " vlmNr: " + vlmNrInt
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("ApiContext does not have enough privileges");
        }
        return new Pair<>(writecacheRscData, children);
    }

    @Override
    public void persist(WritecacheRscData<?> writecacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if WritecacheRscData will get a database table in future.
    }

    @Override
    public void delete(WritecacheRscData<?> writecacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if WritecacheRscData will get a database table in future.
    }

    @Override
    public void persist(WritecacheVlmData<?> writecacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating WritecacheVlmData %s", getId(writecacheVlmDataRef));
        update(writecacheVlmDataRef);
    }

    private void update(WritecacheVlmData<?> writecacheVlmDataRef)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        StorPool extStorPool = writecacheVlmDataRef.getCacheStorPool();

        String nodeName = null;
        String poolName = null;

        if (extStorPool != null)
        {
            nodeName = extStorPool.getNode().getName().value;
            poolName = extStorPool.getName().value;
        }

        tx.update(
            GeneratedDatabaseTables.LAYER_WRITECACHE_VOLUMES,
            GenCrdCurrent.createLayerWritecacheVolumes(
                writecacheVlmDataRef.getRscLayerId(),
                writecacheVlmDataRef.getVlmNr().value,
                nodeName,
                poolName
            )
        );
    }

    @Override
    public void delete(WritecacheVlmData<?> writecacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting WritecacheVlmData %s", getId(writecacheVlmDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.update(
            GeneratedDatabaseTables.LAYER_WRITECACHE_VOLUMES,
            GenCrdCurrent.createLayerWritecacheVolumes(
                writecacheVlmDataRef.getRscLayerId(),
                writecacheVlmDataRef.getVlmNr().value,
                null,
                null
            )
        );
    }
}
