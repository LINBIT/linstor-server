package com.linbit.linstor.core.objects;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerCacheVolumes;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.CacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class CacheLayerETCDDriver extends BaseEtcdDriver implements CacheLayerCtrlDatabaseDriver
{
    private static final int PK_V_LRI_ID_IDX = 0;
    private static final int PK_V_VLM_NR_IDX = 1;

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;

    @Inject
    public CacheLayerETCDDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
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

        Map<String, String> etcdVlmMap = namespace(
            GeneratedDatabaseTables.LAYER_CACHE_VOLUMES
        )
            .get(true);
        Set<String> composedPkSet = EtcdUtils.getComposedPkList(etcdVlmMap);
        NodeName nodeName = absRsc.getNode().getName();

        try
        {
            for (String composedPk : composedPkSet)
            {
                String[] pks = EtcdUtils.splitPks(composedPk, false);

                vlmNrInt = Integer.parseInt(pks[PK_V_VLM_NR_IDX]);
                String cacheStorPoolNameStr = get(etcdVlmMap, LayerCacheVolumes.POOL_NAME_CACHE, pks);
                String metaStorPoolNameStr = get(etcdVlmMap, LayerCacheVolumes.POOL_NAME_META, pks);

                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);
                StorPool cacheStorPool = tmpStorPoolMapRef.get(
                    new Pair<>(
                        nodeName,
                        new StorPoolName(cacheStorPoolNameStr)
                    )
                ).objA;
                StorPool metaStorPool = tmpStorPoolMapRef.get(
                    new Pair<>(
                        nodeName,
                        new StorPoolName(metaStorPoolNameStr)
                    )
                ).objA;

                vlmMap.put(
                    vlm.getVolumeNumber(),
                    new CacheVlmData<>(
                        vlm,
                        cacheRscData,
                        cacheStorPool,
                        metaStorPool,
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
        StorPool cacheStorPool = cacheVlmDataRef.getCacheStorPool();
        StorPool metaStorPool = cacheVlmDataRef.getMetaStorPool();
        getNamespace(cacheVlmDataRef)
            .put(LayerCacheVolumes.NODE_NAME, cacheStorPool.getNode().getName().value)
            .put(LayerCacheVolumes.POOL_NAME_CACHE, cacheStorPool.getName().value)
            .put(LayerCacheVolumes.POOL_NAME_META, metaStorPool.getName().value);
    }

    @Override
    public void delete(CacheVlmData<?> cacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting CacheVlmData %s", getId(cacheVlmDataRef));
        getNamespace(cacheVlmDataRef)
            .delete(true);
    }

    private String getId(CacheRscData<?> cacheRscData)
    {
        return "(LayerRscId=" + cacheRscData.getRscLayerId() +
            ", SuffResName=" + cacheRscData.getSuffixedResourceName() +
            ")";
    }

    private String getId(CacheVlmData<?> cacheVlmData)
    {
        return "(LayerRscId=" + cacheVlmData.getRscLayerId() +
            ", SuffResName=" + cacheVlmData.getRscLayerObject().getSuffixedResourceName() +
            ", VlmNr=" + cacheVlmData.getVlmNr().value +
            ")";
    }

    private FluentLinstorTransaction getNamespace(CacheVlmData<?> cacheVlmDataRef)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_CACHE_VOLUMES,
            Integer.toString(cacheVlmDataRef.getRscLayerId()),
            Integer.toString(cacheVlmDataRef.getVlmNr().value)
        );
    }

    private String get(Map<String, String> map, Column col, String... pks)
    {
        return map.get(EtcdUtils.buildKey(col, pks));
    }

}
