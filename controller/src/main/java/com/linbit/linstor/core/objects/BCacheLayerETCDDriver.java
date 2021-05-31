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
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerBcacheVolumes;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.BCacheLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
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
import java.util.UUID;

@Singleton
public class BCacheLayerETCDDriver extends BaseEtcdDriver implements BCacheLayerCtrlDatabaseDriver
{
    private static final int PK_V_VLM_NR_IDX = 1;

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;

    private final SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> vlmDevUuidDriver;

    @Inject
    public BCacheLayerETCDDriver(
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

        vlmDevUuidDriver = new VlmDevUuidDriver();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver()
    {
        return vlmDevUuidDriver;
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

        Map<String, String> etcdVlmMap = namespace(GeneratedDatabaseTables.LAYER_WRITECACHE_VOLUMES)
            .get(true);
        Set<String> composedPkSet = EtcdUtils.getComposedPkList(etcdVlmMap);
        NodeName nodeName = absRsc.getNode().getName();

        try
        {
            for (String composedPk : composedPkSet)
            {
                String[] pks = EtcdUtils.splitPks(composedPk, false);

                vlmNrInt = Integer.parseInt(pks[PK_V_VLM_NR_IDX]);
                String cacheStorPoolNameStr = get(etcdVlmMap, LayerBcacheVolumes.POOL_NAME, pks);

                VolumeNumber vlmNr = new VolumeNumber(vlmNrInt);

                AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);
                StorPool cacheStorPool = tmpStorPoolMapRef.get(
                    new Pair<>(
                        nodeName,
                        new StorPoolName(cacheStorPoolNameStr)
                    )
                ).objA;

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
        StorPool extStorPool = bcacheVlmDataRef.getCacheStorPool();
        getNamespace(bcacheVlmDataRef)
            .put(LayerBcacheVolumes.NODE_NAME, extStorPool.getNode().getName().value)
            .put(LayerBcacheVolumes.POOL_NAME, extStorPool.getName().value);
    }

    @Override
    public void delete(BCacheVlmData<?> bcacheVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting BCacheVlmData %s", getId(bcacheVlmDataRef));
        getNamespace(bcacheVlmDataRef)
            .delete(true);
    }

    private String getId(BCacheVlmData<?> bcacheVlmData)
    {
        return "(LayerRscId=" + bcacheVlmData.getRscLayerId() +
            ", SuffResName=" + bcacheVlmData.getRscLayerObject().getSuffixedResourceName() +
            ", VlmNr=" + bcacheVlmData.getVlmNr().value +
            ")";
    }

    private FluentLinstorTransaction getNamespace(BCacheVlmData<?> bcacheVlmDataRef)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_WRITECACHE_VOLUMES,
            Integer.toString(bcacheVlmDataRef.getRscLayerId()),
            Integer.toString(bcacheVlmDataRef.getVlmNr().value)
        );
    }

    private String get(Map<String, String> map, Column col, String... pks)
    {
        return map.get(EtcdUtils.buildKey(col, pks));
    }

    private class VlmDevUuidDriver implements SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID>
    {
        @Override
        public void update(BCacheVlmData<?> bcacheVlmData, UUID uuid) throws DatabaseException
        {
            String fromStr = null;
            String toStr = uuid.toString();
            if (bcacheVlmData.getDeviceUuid() != null)
            {
                fromStr = bcacheVlmData.getDeviceUuid().toString();
            }
            errorReporter.logTrace(
                "Updating BCacheVlmData's device UUID from [%s] to [%s] %s",
                fromStr,
                toStr,
                getId(bcacheVlmData)
            );
            getNamespace(bcacheVlmData)
                .put(LayerBcacheVolumes.DEV_UUID, uuid.toString());
        }
    }

}
