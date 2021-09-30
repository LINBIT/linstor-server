package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.OpenflexLayerSQLDbDriver.OpenflexVlmInfo;
import com.linbit.linstor.core.objects.StorPool.InitMaps;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerOpenflexResourceDefinitions;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerOpenflexVolumes;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeVlmData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscDfnData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;
import com.linbit.linstor.utils.NameShortener;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class OpenflexLayerETCDDriver extends BaseEtcdDriver implements OpenflexLayerCtrlDatabaseDriver
{
    private static final int PK_RD_RSC_NAME_IDX = 0;
    private static final int PK_RD_RSC_NAME_SUFFIX_IDX = 1;
    private static final int PK_RD_SNAP_NAME_IDX = 2;

    private static final int PK_V_LRI_ID_IDX = 0;
    private static final int PK_V_VLM_NR_IDX = 1;

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final NameShortener nameShortener;

    private final NqnDriver nqnDriver;

    // key is layerRscId
    private Map<Integer, List<OpenflexVlmInfo>> cachedVlmInfoMap;
    private Map<Pair<ResourceDefinition, String>, Pair<OpenflexRscDfnData<Resource>, ArrayList<OpenflexRscData<Resource>>>> cacheRscDfnDataMap;

    @Inject
    public OpenflexLayerETCDDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrETCD> transMgrProviderRef,
        @Named(NameShortener.OPENFLEX) NameShortener nameShortenerRef
    )
    {
        super(transMgrProviderRef);
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        nameShortener = nameShortenerRef;

        nqnDriver = new NqnDriver();
    }

    @Override
    public ResourceLayerIdDatabaseDriver getIdDriver()
    {
        return idDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> getNqnDriver() throws DatabaseException
    {
        return nqnDriver;
    }

    @Override
    public void clearLoadAllCache()
    {
        cachedVlmInfoMap.clear();
        cachedVlmInfoMap = null;

        cacheRscDfnDataMap.clear();
        cacheRscDfnDataMap = null;
    }

    @Override
    public void fetchForLoadAll(
        Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> tmpStorPoolMapRef,
        Map<ResourceName, ResourceDefinition> rscDfnMap
    )
        throws DatabaseException
    {
        fetchOfRscDfns(rscDfnMap);
        fetchOfVlms(tmpStorPoolMapRef);
    }

    private void fetchOfRscDfns(Map<ResourceName, ResourceDefinition> rscDfnMap) throws DatabaseException
    {
        cacheRscDfnDataMap = new HashMap<>();
        try
        {
            Map<String, String> allOfRscDfnMap = namespace(GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS)
                .get(true);

            Set<String> composedPkSet = EtcdUtils.getComposedPkList(allOfRscDfnMap);
            for (String composedPk : composedPkSet)
            {
                String[] pks = EtcdUtils.splitPks(composedPk, false);
                String rscDfnStr = pks[PK_RD_RSC_NAME_IDX];
                String rscNameSuffix = pks[PK_RD_RSC_NAME_SUFFIX_IDX];
                // String snapDfnStr = pks[PK_RD_SNAP_NAME_IDX];

                ResourceName rscName = new ResourceName(rscDfnStr);

                ResourceDefinition rscDfn;
                // SnapshotDefinition snapDfn;
                OpenflexRscDfnData<Resource> ofRscDfnData;

                // boolean restoreAsResourceDefinition = snapDfnStr == null || snapDfnStr.isEmpty();
                //
                // if (restoreAsResourceDefinition)
                // {
                    rscDfn = rscDfnMap.get(rscName);
                    ofRscDfnData = rscDfn.getLayerData(dbCtx, DeviceLayerKind.OPENFLEX, rscNameSuffix);
                // }
                // else
                // {
                    // ignored / currently not supported
                // }

                if (ofRscDfnData == null)
                {
                    String nqn = allOfRscDfnMap.get(
                        EtcdUtils.buildKey(LayerOpenflexResourceDefinitions.NQN, pks)
                    );

                    ArrayList<OpenflexRscData<Resource>> rscDataList = new ArrayList<>();

                    try
                    {
                        ofRscDfnData = new OpenflexRscDfnData<>(
                            rscDfn.getName(),
                            rscNameSuffix,
                            nameShortener.shorten(rscDfn, rscNameSuffix),
                            rscDataList,
                            nqn,
                            this,
                            transObjFactory,
                            transMgrProvider
                        );
                    }
                    catch (LinStorException lsExc)
                    {
                        throw new ImplementationError(
                            "Cannot reload Openflex resource definition from etcd key/value store",
                            lsExc
                        );
                    }
                    cacheRscDfnDataMap.put(
                        new Pair<>(
                            rscDfn,
                            rscNameSuffix
                        ),
                        new Pair<>(
                            ofRscDfnData,
                            rscDataList
                        )
                    );
                    rscDfn.setLayerData(dbCtx, ofRscDfnData);
                }
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new LinStorDBRuntimeException(
                "Failed to restore stored resourceName [" + invalidNameExc.invalidName + "]",
                invalidNameExc
            );
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accessDeniedExc);
        }
    }

    private void fetchOfVlms(Map<Pair<NodeName, StorPoolName>, Pair<StorPool, InitMaps>> tmpStorPoolMapRef) throws DatabaseException
    {
        cachedVlmInfoMap = new HashMap<>();

        Map<String, String> allVlmDataMap = namespace(GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES)
            .get(true);
        Set<String> composedPkSet = EtcdUtils.getComposedPkList(allVlmDataMap);
        int rscLayerId = -1;
        try
        {
            for (String composedPk : composedPkSet)
            {
                String[] pks = EtcdUtils.splitPks(composedPk, false);
                rscLayerId = Integer.parseInt(pks[PK_V_LRI_ID_IDX]);
                int vlmNr = Integer.parseInt(pks[PK_V_VLM_NR_IDX]);

                List<OpenflexVlmInfo> infoList = cachedVlmInfoMap.get(rscLayerId);
                if (infoList == null)
                {
                    infoList = new ArrayList<>();
                    cachedVlmInfoMap.put(rscLayerId, infoList);
                }
                NodeName nodeName = new NodeName(
                    allVlmDataMap.get(
                        EtcdUtils.buildKey(LayerOpenflexVolumes.NODE_NAME, pks)
                    )
                );
                StorPoolName storPoolName = new StorPoolName(
                    allVlmDataMap.get(
                        EtcdUtils.buildKey(LayerOpenflexVolumes.POOL_NAME, pks)
                    )
                );

                Pair<StorPool, StorPool.InitMaps> storPoolWithInitMap = tmpStorPoolMapRef.get(
                    new Pair<>(nodeName, storPoolName)
                );
                infoList.add(
                    new OpenflexVlmInfo(
                        rscLayerId,
                        vlmNr,
                        storPoolWithInitMap.objA,
                        storPoolWithInitMap.objB
                    )
                );
            }
        }
        catch (InvalidNameException exc)
        {
            throw new LinStorDBRuntimeException(
                String.format(
                    "Failed to restore stored name '%s' of (layered) resource id: %d",
                    exc.invalidName,
                    rscLayerId
                )
            );
        }
    }

    /**
     * Fully loads a {@link NvmeRscData} object including its {@link NvmeVlmData}
     *
     * @param parentRef
     * @return a {@link Pair}, where the first object is the actual NvmeRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<OpenflexRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
    {
        Set<AbsRscLayerObject<RSC>> children = new HashSet<>();
        Map<VolumeNumber, OpenflexVlmData<RSC>> vlmMap = new TreeMap<>();

        Pair<OpenflexRscDfnData<Resource>, ArrayList<OpenflexRscData<Resource>>> rscDfnDataPair = cacheRscDfnDataMap
            .get(
                new Pair<>(
                    absRsc.getResourceDefinition(),
                    rscSuffixRef
                )
            );

        OpenflexRscData<RSC> ofRscData = new OpenflexRscData<>(
            id,
            absRsc,
            (OpenflexRscDfnData<RSC>) rscDfnDataPair.objA, // FIXME as soon as snapshots are supported for openflex
            parentRef,
            children,
            vlmMap,
            this,
            transObjFactory,
            transMgrProvider
        );
        rscDfnDataPair.objB.add((OpenflexRscData<Resource>) ofRscData);// FIXME as soon as snapshots are supported for
                                                                       // openflex

        List<OpenflexVlmInfo> ofVlmInfoList = cachedVlmInfoMap.get(id);
        try
        {
            for (OpenflexVlmInfo ofVlmInfo : ofVlmInfoList)
            {
                VolumeNumber vlmNr = new VolumeNumber(ofVlmInfo.vlmNr);
                AbsVolume<RSC> vlm = absRsc.getVolume(vlmNr);

                vlmMap.put(
                    vlmNr,
                    new OpenflexVlmData<>(vlm, ofRscData, ofVlmInfo.storPool, transObjFactory, transMgrProvider)
                );
            }
        }
        catch (ValueOutOfRangeException exc)
        {
            throw new ImplementationError("Invalid volume number loaded", exc);
        }
        return new Pair<>(ofRscData, children);
    }

    @Override
    public void create(OpenflexRscDfnData<?> ofRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Creating OpenflexRscData %s", getId(ofRscDfnData));
        if (ofRscDfnData.getNqn() != null)
        {
            getNamespace(ofRscDfnData)
                .put(LayerOpenflexResourceDefinitions.NQN, ofRscDfnData.getNqn());
        }
    }

    @Override
    public void delete(OpenflexRscDfnData<?> ofRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting OpenflexRscDfnData %s", getId(ofRscDfnData));
        getNamespace(ofRscDfnData)
            .delete(true);
    }

    @Override
    public void create(OpenflexRscData<?> ofRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if OpenflexRscData will get a database table in future.
    }

    @Override
    public void delete(OpenflexRscData<?> ofRscDataRef) throws DatabaseException
    {
        // no-op - there is no special database table.
        // this method only exists if OpenflexRscData will get a database table in future.
    }

    @Override
    public void persist(OpenflexVlmData<?> ofVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Creating OpenflexVlmData %s", getId(ofVlmDataRef));
        StorPool storPool = ofVlmDataRef.getStorPool();
        getNamespace(ofVlmDataRef)
            .put(LayerOpenflexVolumes.NODE_NAME, storPool.getNode().getName().value)
            .put(LayerOpenflexVolumes.POOL_NAME, storPool.getName().value);
    }

    @Override
    public void delete(OpenflexVlmData<?> ofVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting OpenflexVlmData %s", getId(ofVlmDataRef));
        getNamespace(ofVlmDataRef)
            .delete(true);
    }

    private FluentLinstorTransaction getNamespace(OpenflexVlmData<?> ofVlmData)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES,
            Integer.toString(ofVlmData.getRscLayerId()),
            Integer.toString(ofVlmData.getVlmNr().value)
        );
    }

    private FluentLinstorTransaction getNamespace(OpenflexRscDfnData<?> ofRscDfnData)
    {
        return namespace(
            GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS,
            ofRscDfnData.getResourceName().value,
            ofRscDfnData.getRscNameSuffix()
        );
    }

    private class NqnDriver implements SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String>
    {
        @Override
        public void update(OpenflexRscDfnData<?> ofRscDfnData, String nqn) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating OpenflexRscDfnData's secret from [%s] to [%s] %s",
                ofRscDfnData.getNqn(),
                nqn,
                getId(ofRscDfnData)
            );
            FluentLinstorTransaction namespace = getNamespace(ofRscDfnData);
            if (ofRscDfnData.getNqn() != null)
            {
                namespace.put(LayerOpenflexResourceDefinitions.NQN, ofRscDfnData.getNqn());
            }
            else
            {
                namespace(
                    EtcdUtils.buildKey(
                        LayerOpenflexResourceDefinitions.NQN,
                        ofRscDfnData.getResourceName().value,
                        ofRscDfnData.getRscNameSuffix()
                    )
                ).delete(false);
            }
        }
    }
}
