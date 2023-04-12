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
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent.LayerCacheVolumesSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
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
public class CacheLayerK8sCrdDriver implements CacheLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final LayerResourceIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    private final HashMap<Integer, HashMap<Integer, GenCrdCurrent.LayerCacheVolumesSpec>> vlmSpecCache;


    @Inject
    public CacheLayerK8sCrdDriver(
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

        vlmSpecCache = new HashMap<>();
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public void fetchForLoadAll()
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, GenCrdCurrent.LayerCacheVolumesSpec> cacheVlmSpecMap = tx.getSpec(
            GeneratedDatabaseTables.LAYER_CACHE_VOLUMES
        );
        for (GenCrdCurrent.LayerCacheVolumesSpec cacheVlmSpec : cacheVlmSpecMap.values())
        {
            HashMap<Integer, GenCrdCurrent.LayerCacheVolumesSpec> map = vlmSpecCache.get(cacheVlmSpec.layerResourceId);
            if (map == null)
            {
                map = new HashMap<>();
                vlmSpecCache.put(cacheVlmSpec.layerResourceId, map);
            }
            map.put(cacheVlmSpec.vlmNr, cacheVlmSpec);
        }
    }

    @Override
    public void clearLoadAllCache()
    {
        vlmSpecCache.clear();
    }

    /**
     * Fully loads a {@link CacheRscData} object including its {@link CacheVlmData}
     *
     * @param parentRef
     * @return a {@link Pair}, where the first object is the actual CacheRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     * @throws DatabaseException
     */
    @Override
    public <
        RSC extends AbsResource<RSC>> Pair<CacheRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef,
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> tmpStorPoolMapRef
    )
        throws DatabaseException
    {
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, CacheVlmData<RSC>> vlmMap = new TreeMap<>();
        CacheRscData<RSC> cacheRscData = new CacheRscData<>(
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
            Map<Integer, LayerCacheVolumesSpec> vlmSpecsMap = K8sCrdUtils.getCheckedVlmMap(
                dbCtx,
                absRsc,
                vlmSpecCache,
                id
            );

            for (Entry<Integer, GenCrdCurrent.LayerCacheVolumesSpec> entry : vlmSpecsMap.entrySet())
            {
                vlmNrInt = entry.getKey();
                LayerCacheVolumesSpec cacheVlmSpec = entry.getValue();
                String cacheStorPoolNameStr = cacheVlmSpec.poolNameCache;
                String metaStorPoolNameStr = cacheVlmSpec.poolNameMeta;

                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                NodeName nodeName = new NodeName(cacheVlmSpec.nodeName);

                AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);
                StorPool cacheStorPool = null;
                StorPool metaStorPool = null;
                if (cacheStorPoolNameStr != null && metaStorPoolNameStr != null)
                {
                    cacheStorPool = tmpStorPoolMapRef.get(
                        new Pair<>(
                            nodeName,
                            new StorPoolName(cacheStorPoolNameStr)
                        )
                    ).objA;
                    metaStorPool = tmpStorPoolMapRef.get(
                        new Pair<>(
                            nodeName,
                            new StorPoolName(metaStorPoolNameStr)
                        )
                    ).objA;
                }

                vlmMap.put(
                    vlm.getVolumeNumber(),
                    new CacheVlmData<>(
                        vlm,
                        cacheRscData,
                        cacheStorPool,
                        metaStorPool,
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
                    " for resource layer id: " + cacheRscData
                        .getRscLayerId()
            );
        }
        catch (InvalidNameException exc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored storage pool name '" + exc.invalidName +
                    "' for resource layer id " + cacheRscData.getRscLayerId() + " vlmNr: " +
                    vlmNrInt
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("ApiContext does not have enough privileges");
        }
        return new Pair<>(cacheRscData, children);
    }

    @Override
    public void persist(CacheRscData<?> cacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if CacheRscData will get a database table in future.
    }

    @Override
    public void delete(CacheRscData<?> cacheRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if CacheRscData will get a database table in future.
    }

    @Override
    public void persist(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating CacheVlmData %s", getId(cacheVlmDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        StorPool cacheStorPool = cacheVlmDataRef.getCacheStorPool();
        StorPool metaStorPool = cacheVlmDataRef.getMetaStorPool();
        String nodeName = null;
        String cachePoolName = null;
        String metaPoolName = null;
        if (cacheStorPool != null && metaStorPool != null)
        {
            nodeName = cacheStorPool.getNode().getName().value;
            cachePoolName = cacheStorPool.getName().value;
            metaPoolName = metaStorPool.getName().value;
        }
        tx.create(
            GeneratedDatabaseTables.LAYER_CACHE_VOLUMES,
            GenCrdCurrent.createLayerCacheVolumes(
                cacheVlmDataRef.getRscLayerId(),
                cacheVlmDataRef.getVlmNr().value,
                nodeName,
                cachePoolName,
                metaPoolName
            )
        );
    }

    @Override
    public void delete(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting CacheVlmData %s", getId(cacheVlmDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_CACHE_VOLUMES,
            GenCrdCurrent.createLayerCacheVolumes(
                cacheVlmDataRef.getRscLayerId(),
                cacheVlmDataRef.getVlmNr().value,
                null,
                null,
                null
            )
        );
    }
}
