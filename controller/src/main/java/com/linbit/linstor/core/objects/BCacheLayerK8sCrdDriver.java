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
import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.LayerBcacheVolumesSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
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
import java.util.UUID;

@Singleton
public class BCacheLayerK8sCrdDriver implements BCacheLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final LayerResourceIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    private final SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> vlmDevUuidDriver;

    private final HashMap<Integer, HashMap<Integer, GenCrdCurrent.LayerBcacheVolumesSpec>> vlmSpecCache;

    @Inject
    public BCacheLayerK8sCrdDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        LayerResourceIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        vlmDevUuidDriver = (bcacheVlmData, ignored) -> insertOrUpdate(bcacheVlmData, false);

        vlmSpecCache = new HashMap<>();
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver()
    {
        return vlmDevUuidDriver;
    }

    @Override
    public void fetchForLoadAll()
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, GenCrdCurrent.LayerBcacheVolumesSpec> bcacheVlmSpecMap = tx.getSpec(
            GeneratedDatabaseTables.LAYER_BCACHE_VOLUMES
        );
        for (GenCrdCurrent.LayerBcacheVolumesSpec bcacheVlmSpec : bcacheVlmSpecMap.values())
        {
            HashMap<Integer, GenCrdCurrent.LayerBcacheVolumesSpec> map = vlmSpecCache.get(
                bcacheVlmSpec.layerResourceId
            );
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
     * Fully loads a {@link BCacheRscData} object including its {@link BCacheVlmData}
     *
     * @param parentRef
     *
     * @return a {@link Pair}, where the first object is the actual BCacheRscData and the second object
     *         is the first objects backing list of the children-resource layer data. This list is expected to be filled
     *         upon further loading, without triggering transaction (and possibly database-) updates.
     *
     * @throws DatabaseException
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<BCacheRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException
    {
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, BCacheVlmData<RSC>> vlmMap = new TreeMap<>();
        BCacheRscData<RSC> bcacheRscData = new BCacheRscData<>(
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
            Map<Integer, LayerBcacheVolumesSpec> vlmSpecsMap = K8sCrdUtils.getCheckedVlmMap(
                dbCtx,
                absRsc,
                vlmSpecCache,
                id
            );

            for (Entry<Integer, LayerBcacheVolumesSpec> entry : vlmSpecsMap.entrySet())
            {
                vlmNrInt = entry.getKey();
                LayerBcacheVolumesSpec vlmSpec = entry.getValue();
                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);
                StorPool cacheStorPool = null;
                if (vlmSpec.nodeName != null && vlmSpec.poolName != null)
                {
                    cacheStorPool = tmpStorPoolMapRef.get(
                        new Pair<>(
                            new NodeName(vlmSpec.nodeName),
                            new StorPoolName(vlmSpec.poolName)
                        )
                    ).objA;
                }

                vlmMap.put(
                    vlm.getVolumeNumber(),
                    new BCacheVlmData<>(
                        vlm,
                        bcacheRscData,
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
                    " for resource layer id: " + bcacheRscData.getRscLayerId()
            );
        }
        catch (InvalidNameException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored storage pool name '" + exc.invalidName +
                    "' for resource layer id " + bcacheRscData.getRscLayerId() + " vlmNr: " + vlmNrInt
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("ApiContext does not have enough privileges");
        }
        return new Pair<>(bcacheRscData, children);
    }

    @Override
    public void persist(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if BCacheRscData will get a database table in future.
    }

    @Override
    public void delete(BCacheRscData<?> bcacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if BCacheRscData will get a database table in future.
    }

    @Override
    public void persist(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating BCacheVlmData %s", getId(bcacheVlmDataRef));
        insertOrUpdate(bcacheVlmDataRef, true);
    }

    private void insertOrUpdate(BCacheVlmData<?> bcacheVlmDataRef, boolean isNew)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        StorPool extStorPool = bcacheVlmDataRef.getCacheStorPool();
        String nodeName = null;
        String poolName = null;
        if (extStorPool != null)
        {
            nodeName = extStorPool.getNode().getName().value;
            poolName = extStorPool.getName().value;
        }

        GenCrdCurrent.LayerBcacheVolumes val = GenCrdCurrent.createLayerBcacheVolumes(
            bcacheVlmDataRef.getRscLayerId(),
            bcacheVlmDataRef.getVlmNr().value,
            nodeName,
            poolName,
            bcacheVlmDataRef.getDeviceUuid() == null ? null : bcacheVlmDataRef.getDeviceUuid().toString()
        );

        tx.createOrReplace(GeneratedDatabaseTables.LAYER_BCACHE_VOLUMES, val, isNew);
    }

    @Override
    public void delete(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting BCacheVlmData %s", getId(bcacheVlmDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_BCACHE_VOLUMES,
            GenCrdCurrent.createLayerBcacheVolumes(
                bcacheVlmDataRef.getRscLayerId(),
                bcacheVlmDataRef.getVlmNr().value,
                null,
                null,
                null
            )
        );
    }
}
