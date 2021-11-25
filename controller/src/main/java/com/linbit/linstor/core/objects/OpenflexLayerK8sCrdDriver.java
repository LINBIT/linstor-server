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
import com.linbit.linstor.core.objects.db.utils.K8sCrdUtils;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.OpenflexLayerCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceLayerIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
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
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.linstor.utils.NameShortener;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class OpenflexLayerK8sCrdDriver implements OpenflexLayerCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final ResourceLayerIdDatabaseDriver idDriver;
    private final TransactionObjectFactory transObjFactory;
    private final NameShortener nameShortener;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    private final SingleColumnDatabaseDriver<OpenflexRscDfnData<?>, String> nqnDriver;

    // key is layerRscId
    private HashMap<Integer, HashMap<Integer, OpenflexVlmInfo>> cachedVlmInfoMap;
    private Map<Pair<ResourceDefinition, String>, Pair<OpenflexRscDfnData<Resource>, ArrayList<OpenflexRscData<Resource>>>> cacheRscDfnDataMap;


    @Inject
    public OpenflexLayerK8sCrdDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        ResourceLayerIdDatabaseDriver idDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef,
        @Named(NameShortener.OPENFLEX) NameShortener nameShortenerRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        idDriver = idDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nameShortener = nameShortenerRef;

        nqnDriver = (nvmeRscDfnData, ignored) -> update(nvmeRscDfnData);
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
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        try
        {
            Map<String, GenCrdCurrent.LayerOpenflexResourceDefinitionsSpec> openflexRscDfnSpecMap = tx.get(
                GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS
            );
            for (GenCrdCurrent.LayerOpenflexResourceDefinitionsSpec ofRscDfnSpec : openflexRscDfnSpecMap.values())
            {
                String rscDfnStr = ofRscDfnSpec.resourceName;
                String rscNameSuffix = ofRscDfnSpec.resourceNameSuffix;
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
                    String nqn = ofRscDfnSpec.nqn;

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
                            "Cannot reload Openflex resource definition from k8s",
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
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, GenCrdCurrent.LayerOpenflexVolumesSpec> openflexVlmSpecMap = tx.get(
            GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES
        );
        int rscLayerId = -1;
        try
        {
            for (GenCrdCurrent.LayerOpenflexVolumesSpec ofVlmSpec : openflexVlmSpecMap.values())
            {
                rscLayerId = ofVlmSpec.layerResourceId;
                int vlmNr = ofVlmSpec.vlmNr;

                HashMap<Integer, OpenflexVlmInfo> infoMap = cachedVlmInfoMap.get(rscLayerId);
                if (infoMap == null)
                {
                    infoMap = new HashMap<>();
                    cachedVlmInfoMap.put(rscLayerId, infoMap);
                }
                NodeName nodeName = new NodeName(ofVlmSpec.nodeName);
                StorPoolName storPoolName = new StorPoolName(ofVlmSpec.poolName);

                Pair<StorPool, StorPool.InitMaps> storPoolWithInitMap = tmpStorPoolMapRef.get(
                    new Pair<>(nodeName, storPoolName)
                );
                infoMap.put(
                    vlmNr,
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
     *
     * @return a {@link Pair}, where the first object is the actual NvmeRscData and the second object
     * is the first objects backing list of the children-resource layer data. This list is expected to be filled
     * upon further loading, without triggering transaction (and possibly database-) updates.
     *
     * @throws DatabaseException
     */
    @Override
    public <RSC extends AbsResource<RSC>> Pair<OpenflexRscData<RSC>, Set<AbsRscLayerObject<RSC>>> load(
        RSC absRsc,
        int id,
        String rscSuffixRef,
        AbsRscLayerObject<RSC> parentRef
    )
        throws DatabaseException
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

        try
        {
            Map<Integer, OpenflexVlmInfo> ofVlmInfoMap = K8sCrdUtils.getCheckedVlmMap(
                dbCtx,
                absRsc,
                cachedVlmInfoMap,
                id
            );
            for (OpenflexVlmInfo ofVlmInfo : ofVlmInfoMap.values())
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
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("ApiContext does not have enough privileges");
        }
        return new Pair<>(ofRscData, children);
    }

    @Override
    public void create(OpenflexRscDfnData<?> ofRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Creating OpenflexRscData %s", getId(ofRscDfnData));
        update(ofRscDfnData);
    }

    private void update(OpenflexRscDfnData<?> ofRscDfnData)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.update(
            GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS,
            GenCrdCurrent.createLayerOpenflexResourceDefinitions(
                ofRscDfnData.getResourceName().value,
                ofRscDfnData.getSnapshotName() == null ? null : ofRscDfnData.getSnapshotName().value,
                ofRscDfnData.getRscNameSuffix(),
                ofRscDfnData.getNqn()
            )
        );
    }

    @Override
    public void delete(OpenflexRscDfnData<?> ofRscDfnData) throws DatabaseException
    {
        errorReporter.logTrace("Deleting OpenflexRscDfnData %s", getId(ofRscDfnData));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_OPENFLEX_RESOURCE_DEFINITIONS,
            GenCrdCurrent.createLayerOpenflexResourceDefinitions(
                ofRscDfnData.getResourceName().value,
                ofRscDfnData.getSnapshotName() == null ? null : ofRscDfnData.getSnapshotName().value,
                ofRscDfnData.getRscNameSuffix(),
                null
            )
        );
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
        update(ofVlmDataRef);
    }

    private void update(OpenflexVlmData<?> ofVlmDataRef)
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        StorPool storPool = ofVlmDataRef.getStorPool();
        tx.update(
            GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES,
            GenCrdCurrent.createLayerOpenflexVolumes(
                ofVlmDataRef.getRscLayerId(),
                ofVlmDataRef.getVlmNr().value,
                storPool.getNode().getName().value,
                storPool.getName().value
            )
        );
    }

    @Override
    public void delete(OpenflexVlmData<?> ofVlmDataRef) throws DatabaseException
    {
        errorReporter.logTrace("Deleting OpenflexVlmData %s", getId(ofVlmDataRef));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES,
            GenCrdCurrent.createLayerOpenflexVolumes(
                ofVlmDataRef.getRscLayerId(),
                ofVlmDataRef.getVlmNr().value,
                null,
                null
            )
        );
    }
}
