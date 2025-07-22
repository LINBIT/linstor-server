package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ExternalFileMap;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CoreModule.ScheduleMap;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.ScheduleName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.FreeSpaceMgr;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Schedule;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceGroupCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ScheduleCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeGroupCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.remotes.LinstorRemoteCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteCtrlDatabaseDriver;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.AbsRscLayerHelper;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DbCoreObjProtInitializer;
import com.linbit.linstor.security.SecDatabaseLoader;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DatabaseLoader implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    static
    {
        try
        {
            DFLT_SERVICE_INSTANCE_NAME = new ServiceName("GernericDatabaseService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The builtin default service instance name is not a valid ServiceName",
                nameExc
            );
        }
    }

    private final AccessContext dbCtx;
    private final SecDatabaseLoader securityDbLoader;
    private final DbCoreObjProtInitializer dbCoreObjProtInitializer;
    private final PropsCtrlDatabaseDriver propsDriver;
    private final ResourceGroupCtrlDatabaseDriver rscGrpDriver;
    private final NodeCtrlDatabaseDriver nodeDriver;
    private final NetInterfaceCtrlDatabaseDriver netIfDriver;
    private final NodeConnectionCtrlDatabaseDriver nodeConnDriver;
    private final ResourceDefinitionCtrlDatabaseDriver rscDfnDriver;
    private final ResourceCtrlDatabaseDriver rscDriver;
    private final ResourceConnectionCtrlDatabaseDriver rscConnDriver;
    private final VolumeDefinitionCtrlDatabaseDriver vlmDfnDriver;
    private final VolumeCtrlDatabaseDriver vlmDriver;
    private final VolumeConnectionCtrlDatabaseDriver vlmConnDriver;
    private final StorPoolDefinitionCtrlDatabaseDriver storPoolDfnDriver;
    private final StorPoolCtrlDatabaseDriver storPoolDriver;
    private final SnapshotDefinitionCtrlDatabaseDriver snapshotDefinitionDriver;
    private final SnapshotVolumeDefinitionCtrlDatabaseDriver snapshotVolumeDefinitionDriver;
    private final SnapshotCtrlDatabaseDriver snapshotDriver;
    private final SnapshotVolumeCtrlDatabaseDriver snapshotVolumeDriver;
    private final KeyValueStoreCtrlDatabaseDriver keyValueStoreGenericDbDriver;
    private final LayerResourceIdCtrlDatabaseDriver layerRscIdDriver;
    private final Map<DeviceLayerKind, ControllerLayerRscDatabaseDriver> layerDriversMap;
    private final ExternalFileCtrlDatabaseDriver extFileDriver;
    private final S3RemoteCtrlDatabaseDriver s3remoteDriver;
    private final LinstorRemoteCtrlDatabaseDriver linstorRemoteDriver;
    private final EbsRemoteCtrlDatabaseDriver ebsRemoteDriver;
    private final ScheduleCtrlDatabaseDriver scheduleDriver;
    private final Provider<CtrlRscLayerDataFactory> ctrlRscLayerDataHelper;

    private final Props ctrlConf;
    private final Props stltConf;
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.ResourceGroupMap rscGrpMap;
    private final CoreModule.ResourceDefinitionMapExtName rscDfnMapExtName;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;
    private final ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMap;
    private final CoreModule.KeyValueStoreMap keyValueStoreMap;
    private final VolumeGroupCtrlDatabaseDriver vlmGrpDriver;
    private final ExternalFileMap extFileMap;
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final RemoteMap remoteMap;
    private final ScheduleMap scheduleMap;

    @Inject
    public DatabaseLoader(
        @SystemContext AccessContext privCtx,
        SecDatabaseLoader securityDbLoaderRef,
        DbCoreObjProtInitializer dbCoreObjProtInitializerRef,
        PropsCtrlDatabaseDriver propsDriverRef,
        ResourceGroupCtrlDatabaseDriver rscGrpDriverRef,
        NodeCtrlDatabaseDriver nodeDriverRef,
        NetInterfaceCtrlDatabaseDriver netIfDriverRef,
        NodeConnectionCtrlDatabaseDriver nodeConnDriverRef,
        ResourceDefinitionCtrlDatabaseDriver resesourceDefinitionDriverRef,
        ResourceCtrlDatabaseDriver resourceDriverRef,
        ResourceConnectionCtrlDatabaseDriver rscConnDriverRef,
        VolumeGroupCtrlDatabaseDriver vlmGrpDriverRef,
        VolumeDefinitionCtrlDatabaseDriver vlmDfnDriverRef,
        VolumeCtrlDatabaseDriver volumeDriverRef,
        VolumeConnectionCtrlDatabaseDriver vlmConnDriverRef,
        StorPoolDefinitionCtrlDatabaseDriver storPoolDefinitionDriverRef,
        StorPoolCtrlDatabaseDriver storPoolDriverRef,
        SnapshotDefinitionCtrlDatabaseDriver snapshotDefinitionDriverRef,
        SnapshotVolumeDefinitionCtrlDatabaseDriver snapshotVolumeDefinitionDriverRef,
        SnapshotCtrlDatabaseDriver snapshotDriverRef,
        SnapshotVolumeCtrlDatabaseDriver snapshotVolumeDriverRef,
        KeyValueStoreCtrlDatabaseDriver keyValueStoreGenericDbDriverRef,
        LayerResourceIdCtrlDatabaseDriver layerRscIdDriverRef,
        Map<DeviceLayerKind, ControllerLayerRscDatabaseDriver> layerDriversMapRef,
        ExternalFileCtrlDatabaseDriver extFilesDriverRef,
        S3RemoteCtrlDatabaseDriver s3remoteDriverRef,
        LinstorRemoteCtrlDatabaseDriver linstorRemoteDriverRef,
        EbsRemoteCtrlDatabaseDriver ebsRemoteDriverRef,
        ScheduleCtrlDatabaseDriver scheduleDriverRef,
        Provider<CtrlRscLayerDataFactory> ctrlRscLayerDataHelperRef,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        CoreModule.ResourceDefinitionMapExtName rscDfnMapExtNameRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        ControllerCoreModule.FreeSpaceMgrMap freeSpaceMgrMapRef,
        CoreModule.KeyValueStoreMap keyValueStoreMapRef,
        CoreModule.ExternalFileMap extFileMapRef,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        CoreModule.RemoteMap remoteMapRef,
        CoreModule.ScheduleMap scheduleMapRef
    )
    {
        dbCtx = privCtx;
        securityDbLoader = securityDbLoaderRef;
        dbCoreObjProtInitializer = dbCoreObjProtInitializerRef;
        propsDriver = propsDriverRef;
        rscGrpDriver = rscGrpDriverRef;
        nodeDriver = nodeDriverRef;
        netIfDriver = netIfDriverRef;
        nodeConnDriver = nodeConnDriverRef;
        rscDfnDriver = resesourceDefinitionDriverRef;
        rscDriver = resourceDriverRef;
        rscConnDriver = rscConnDriverRef;
        vlmGrpDriver = vlmGrpDriverRef;
        vlmDfnDriver = vlmDfnDriverRef;
        vlmDriver = volumeDriverRef;
        vlmConnDriver = vlmConnDriverRef;
        storPoolDfnDriver = storPoolDefinitionDriverRef;
        storPoolDriver = storPoolDriverRef;
        snapshotDefinitionDriver = snapshotDefinitionDriverRef;
        snapshotVolumeDefinitionDriver = snapshotVolumeDefinitionDriverRef;
        snapshotDriver = snapshotDriverRef;
        snapshotVolumeDriver = snapshotVolumeDriverRef;
        keyValueStoreGenericDbDriver = keyValueStoreGenericDbDriverRef;
        layerRscIdDriver = layerRscIdDriverRef;
        layerDriversMap = layerDriversMapRef;
        extFileDriver = extFilesDriverRef;
        s3remoteDriver = s3remoteDriverRef;
        linstorRemoteDriver = linstorRemoteDriverRef;
        ebsRemoteDriver = ebsRemoteDriverRef;
        scheduleDriver = scheduleDriverRef;
        ctrlRscLayerDataHelper = ctrlRscLayerDataHelperRef;
        ctrlConf = ctrlConfRef;
        stltConf = stltConfRef;

        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        rscGrpMap = rscGrpMapRef;
        rscDfnMapExtName = rscDfnMapExtNameRef;
        storPoolDfnMap = storPoolDfnMapRef;
        freeSpaceMgrMap = freeSpaceMgrMapRef;
        keyValueStoreMap = keyValueStoreMapRef;
        extFileMap = extFileMapRef;
        storPoolResolveHelper = storPoolResolveHelperRef;
        remoteMap = remoteMapRef;
        scheduleMap = scheduleMapRef;

        ArrayList<DeviceLayerKind> layerKindsWithoutDriver = new ArrayList<>();
        for (DeviceLayerKind kind : DeviceLayerKind.values())
        {
            if (!layerDriversMap.containsKey(kind))
            {
                layerKindsWithoutDriver.add(kind);
            }
        }

        if (!layerKindsWithoutDriver.isEmpty())
        {
            throw new ImplementationError(layerKindsWithoutDriver + " have no database drivers!");
        }
    }

    @Override
    public void loadSecurityObjects() throws DatabaseException, InitializationException
    {
        securityDbLoader.loadAll();
        dbCoreObjProtInitializer.initialize();
    }

    /**
     * This method should only be called with an locked reconfiguration write lock
     */
    @Override
    public void loadCoreObjects() throws DatabaseException
    {
        try
        {
            /*
             * After 1.12.4 we are prohibiting mixing LVM with LVM_THIN.
             * If such combination was already in the database, we have to disable that check during DB loading
             * since an error during DB loading time is fatal.
             */
            storPoolResolveHelper.setEnableChecks(false);

            propsDriver.loadAll(null); // will load into cache

            // depends on loaded (cached) props
            ctrlConf.loadAll();
            stltConf.loadAll();

            // load the resource groups
            Map<ResourceGroup, ResourceGroup.InitMaps> loadedRscGroupsMap =
                Collections.unmodifiableMap(rscGrpDriver.loadAll(null));

            // temporary map to restore rscDfn <-> rscGroup relations
            Map<ResourceGroupName, ResourceGroup> tmpRscGroups =
                mapByName(loadedRscGroupsMap, ResourceGroup::getName);

            List<VolumeGroup> vlmGrpList =
                Collections.unmodifiableList(vlmGrpDriver.loadAllAsList(tmpRscGroups));
            for (VolumeGroup vlmGrp : vlmGrpList)
            {
                loadedRscGroupsMap.get(vlmGrp.getResourceGroup()).getVlmGrpMap().put(
                    vlmGrp.getVolumeNumber(),
                    vlmGrp
                );
            }

            // load the main objects (nodes, rscDfns, storPoolDfns, extFiles, remotes, schedules)
            Map<Node, Node.InitMaps> loadedNodesMap =
                Collections.unmodifiableMap(nodeDriver.loadAll(null));
            Map<ResourceDefinition, ResourceDefinition.InitMaps> loadedRscDfnsMap =
                Collections.unmodifiableMap(rscDfnDriver.loadAll(tmpRscGroups));
            Map<StorPoolDefinition, StorPoolDefinition.InitMaps> loadedStorPoolDfnsMap =
                Collections.unmodifiableMap(storPoolDfnDriver.loadAll(null));
            Map<ExternalFile, ExternalFile.InitMaps> loadedExtFilesMap =
                Collections.unmodifiableMap(extFileDriver.loadAll(null));
            Map<S3Remote, S3Remote.InitMaps> loadedS3RemotesMap = Collections
                .unmodifiableMap(s3remoteDriver.loadAll(null));
            Map<LinstorRemote, LinstorRemote.InitMaps> loadedLinstorRemotesMap = Collections
                .unmodifiableMap(linstorRemoteDriver.loadAll(null));
            Map<EbsRemote, EbsRemote.InitMaps> loadedEbsRemotesMap = Collections
                .unmodifiableMap(ebsRemoteDriver.loadAll(null));
            Map<Schedule, Schedule.InitMaps> loadedSchedulesMap = Collections
                .unmodifiableMap(scheduleDriver.loadAll(null));

            // add the rscDfns into the corresponding rscGroup rscDfn-map
            for (ResourceDefinition rscDfn : loadedRscDfnsMap.keySet())
            {
                loadedRscGroupsMap.get(rscDfn.getResourceGroup()).getRscDfnMap()
                    .put(rscDfn.getName(), rscDfn);
            }

            // build temporary maps for easier restoring of the remaining objects
            Map<NodeName, Node> tmpNodesMap =
                mapByName(loadedNodesMap, Node::getName);
            Map<ResourceName, ResourceDefinition> tmpRscDfnMap =
                mapByName(loadedRscDfnsMap, ResourceDefinition::getName);
            Map<StorPoolName, StorPoolDefinition> tmpStorPoolDfnMap =
                mapByName(loadedStorPoolDfnsMap, StorPoolDefinition::getName);
            Map<ExternalFileName, ExternalFile> tmpExtFileMap =
                mapByName(loadedExtFilesMap, ExternalFile::getName);
            Map<RemoteName, AbsRemote> tmpRemoteMap = mapByName(loadedS3RemotesMap, S3Remote::getName);
            tmpRemoteMap.putAll(mapByName(loadedLinstorRemotesMap, LinstorRemote::getName));
            tmpRemoteMap.putAll(mapByName(loadedEbsRemotesMap, EbsRemote::getName));
            Map<ScheduleName, Schedule> tmpScheduleMap = mapByName(loadedSchedulesMap, Schedule::getName);


            // loading net interfaces
            List<NetInterface> loadedNetIfs = netIfDriver.loadAllAsList(tmpNodesMap);
            for (NetInterface netIf : loadedNetIfs)
            {
                Node node = netIf.getNode();
                loadedNodesMap.get(node).getNetIfMap()
                    .put(netIf.getName(), netIf);

                String curStltConnName = node.getProps(dbCtx).getProp(ApiConsts.KEY_CUR_STLT_CONN_NAME);
                if (netIf.getName().value.equalsIgnoreCase(curStltConnName))
                {
                    node.setActiveStltConn(dbCtx, netIf);
                }
            }

            List<NodeConnection> loadedNodeConns = nodeConnDriver.loadAllAsList(tmpNodesMap);
            for (NodeConnection nodeConn : loadedNodeConns)
            {
                Node sourceNode = nodeConn.getSourceNode(dbCtx);
                Node targetNode = nodeConn.getTargetNode(dbCtx);
                loadedNodesMap.get(sourceNode).getNodeConnMap().put(targetNode.getName(), nodeConn);
                loadedNodesMap.get(targetNode).getNodeConnMap().put(sourceNode.getName(), nodeConn);
            }


            // loading storage pools
            Map<StorPool, StorPool.InitMaps> loadedStorPools = Collections.unmodifiableMap(
                storPoolDriver.loadAll(
                    new PairNonNull<>(
                        tmpNodesMap,
                        tmpStorPoolDfnMap
                    )
                )
            );
            for (StorPool storPool : loadedStorPools.keySet())
            {
                loadedNodesMap.get(storPool.getNode()).getStorPoolMap()
                    .put(storPool.getName(), storPool);
                loadedStorPoolDfnsMap.get(storPool.getDefinition(dbCtx)).getStorPoolMap()
                    .put(storPool.getNode().getName(), storPool);
            }
            // loading free space managers
            Map<SharedStorPoolName, FreeSpaceMgr> tmpFreeSpaceMgrMap = storPoolDriver.getAllLoadedFreeSpaceMgrs();

            // loading resources
            Map<AbsResource<Resource>, Resource.InitMaps> loadedAbsResources =
                Collections.unmodifiableMap(rscDriver.loadAll(new PairNonNull<>(tmpNodesMap, tmpRscDfnMap)));
            Map<Resource, Resource.InitMaps> loadedResources = new TreeMap<>(); // casted version of loadedAbsResources
            for (Entry<AbsResource<Resource>, Resource.InitMaps> absEntry : loadedAbsResources.entrySet())
            {
                Resource rsc = (Resource) absEntry.getKey();
                loadedNodesMap.get(rsc.getNode()).getRscMap()
                    .put(rsc.getResourceDefinition().getName(), rsc);
                loadedRscDfnsMap.get(rsc.getResourceDefinition()).getRscMap()
                    .put(rsc.getNode().getName(), rsc);

                loadedResources.put(rsc, absEntry.getValue());
            }

            // temporary resource map
            Map<Pair<NodeName, ResourceName>, Resource> tmpRscMap =
                mapByName(loadedResources, rsc -> new Pair<>(
                    rsc.getNode().getName(),
                    rsc.getResourceDefinition().getName()
                )
            );

            // loading resource connections
            List<ResourceConnection> loadedRscConns = rscConnDriver.loadAllAsList(tmpRscMap);
            for (ResourceConnection rscConn : loadedRscConns)
            {
                Resource sourceResource = rscConn.getSourceResource(dbCtx);
                Resource targetResource = rscConn.getTargetResource(dbCtx);
                loadedResources.get(sourceResource).getRscConnMap().put(targetResource.getKey(), rscConn);
                loadedResources.get(targetResource).getRscConnMap().put(sourceResource.getKey(), rscConn);
            }

            // loading volume definitions
            Map<VolumeDefinition, VolumeDefinition.InitMaps> loadedVlmDfnMap =
                Collections.unmodifiableMap(vlmDfnDriver.loadAll(tmpRscDfnMap));

            for (VolumeDefinition vlmDfn : loadedVlmDfnMap.keySet())
            {
                loadedRscDfnsMap.get(vlmDfn.getResourceDefinition()).getVlmDfnMap()
                    .put(vlmDfn.getVolumeNumber(), vlmDfn);
            }

            // temporary volume definition map
            Map<Pair<ResourceName, VolumeNumber>, VolumeDefinition> tmpVlmDfnMap =
                mapByName(loadedVlmDfnMap, vlmDfn -> new Pair<>(
                    vlmDfn.getResourceDefinition().getName(),
                    vlmDfn.getVolumeNumber()
                )
            );

            // loading volumes
            Map<Volume, Volume.InitMaps> loadedVolumes = Collections.unmodifiableMap(
                vlmDriver.loadAll(
                    new PairNonNull<>(tmpRscMap, tmpVlmDfnMap)
                )
            );

            for (Volume vlm : loadedVolumes.keySet())
            {
                loadedResources.get(vlm.getAbsResource()).getVlmMap()
                    .put(vlm.getVolumeDefinition().getVolumeNumber(), vlm);
                loadedVlmDfnMap.get(vlm.getVolumeDefinition()).getVlmMap()
                    .put(Resource.getStringId(vlm.getAbsResource()), vlm);
            }

            // temporary volume map
            Map<Triple<NodeName, ResourceName, VolumeNumber>, Volume> tmpVlmMap =
                mapByName(loadedVolumes, vlm -> new Triple<>(
                    vlm.getAbsResource().getNode().getName(),
                    vlm.getResourceDefinition().getName(),
                    vlm.getVolumeDefinition().getVolumeNumber()
                )
            );

            List<VolumeConnection> loadedVlmConns = vlmConnDriver.loadAllAsList(tmpVlmMap);
            for (VolumeConnection vlmConn : loadedVlmConns)
            {
                Volume sourceVolume = vlmConn.getSourceVolume(dbCtx);
                Volume targetVolume = vlmConn.getTargetVolume(dbCtx);
                loadedVolumes.get(sourceVolume).getVolumeConnections().put(targetVolume.getKey(), vlmConn);
                loadedVolumes.get(targetVolume).getVolumeConnections().put(sourceVolume.getKey(), vlmConn);
            }

            // loading snapshot definitions
            Map<SnapshotDefinition, SnapshotDefinition.InitMaps> loadedSnapshotDfns = snapshotDefinitionDriver.loadAll(
                tmpRscDfnMap
            );
            for (SnapshotDefinition snapshotDfn : loadedSnapshotDfns.keySet())
            {
                loadedRscDfnsMap.get(snapshotDfn.getResourceDefinition()).getSnapshotDfnMap()
                    .put(snapshotDfn.getName(), snapshotDfn);
            }

            // temporary snapshot definition map
            Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> tmpSnapshotDfnMap =
                mapByName(loadedSnapshotDfns, snapshotDfn -> new Pair<>(
                        snapshotDfn.getResourceName(),
                        snapshotDfn.getName()
                    )
                );

            // loading snapshot volume definitions
            Map<SnapshotVolumeDefinition, SnapshotVolumeDefinition.InitMaps> loadedSnapshotVolumeDefinitions =
                snapshotVolumeDefinitionDriver.loadAll(
                    new PairNonNull<>(tmpSnapshotDfnMap, tmpVlmDfnMap)
                );
            for (SnapshotVolumeDefinition snapshotVolumeDefinition : loadedSnapshotVolumeDefinitions.keySet())
            {
                loadedSnapshotDfns.get(snapshotVolumeDefinition.getSnapshotDefinition())
                    .getSnapshotVolumeDefinitionMap()
                    .put(snapshotVolumeDefinition.getVolumeNumber(), snapshotVolumeDefinition);
            }

            Map<Triple<ResourceName, SnapshotName, VolumeNumber>, SnapshotVolumeDefinition> tmpSnapshotVlmDfnMap =
                mapByName(loadedSnapshotVolumeDefinitions, snapshotVlmDfn -> new Triple<>(
                    snapshotVlmDfn.getResourceName(),
                    snapshotVlmDfn.getSnapshotName(),
                    snapshotVlmDfn.getVolumeNumber()
                )
            );

            // loading snapshots
            Map<AbsResource<Snapshot>, Snapshot.InitMaps> loadedAbsSnapshots = snapshotDriver.loadAll(
                new PairNonNull<>(tmpNodesMap, tmpSnapshotDfnMap)
            );
            Map<Snapshot, Snapshot.InitMaps> loadedSnapshots = new TreeMap<>();
            for (Entry<AbsResource<Snapshot>, Snapshot.InitMaps> absEntry : loadedAbsSnapshots.entrySet())
            {
                Snapshot snapshot = (Snapshot) absEntry.getKey();
                loadedNodesMap.get(snapshot.getNode()).getSnapshotMap()
                    .put(snapshot.getSnapshotDefinition().getSnapDfnKey(), snapshot);
                loadedSnapshotDfns.get(snapshot.getSnapshotDefinition()).getSnapshotMap()
                    .put(snapshot.getNodeName(), snapshot);

                loadedSnapshots.put(snapshot, absEntry.getValue());
            }

            Map<Triple<NodeName, ResourceName, SnapshotName>, Snapshot> tmpSnapshotMap =
                mapByName(loadedSnapshots, snapshot -> new Triple<>(
                    snapshot.getNodeName(),
                    snapshot.getResourceName(),
                    snapshot.getSnapshotName()
                )
            );

            // loading snapshot volumes
            List<SnapshotVolume> loadedSnapshotVolumes =
                snapshotVolumeDriver.loadAllAsList(
                    new PairNonNull<>(
                        tmpSnapshotMap,
                        tmpSnapshotVlmDfnMap
                    )
                );
            for (SnapshotVolume snapshotVolume : loadedSnapshotVolumes)
            {
                loadedSnapshots.get(snapshotVolume.getAbsResource()).getSnapshotVlmMap()
                    .put(snapshotVolume.getVolumeNumber(), snapshotVolume);
                loadedSnapshotVolumeDefinitions.get(snapshotVolume.getSnapshotVolumeDefinition()).getSnapshotVlmMap()
                    .put(snapshotVolume.getNodeName(), snapshotVolume);
            }

            // load and put key value store map
            Map<KeyValueStore, KeyValueStore.InitMaps> loadedKeyValueStoreMap =
                Collections.unmodifiableMap(keyValueStoreGenericDbDriver.loadAll(null));
            Map<KeyValueStoreName, KeyValueStore> tmpKeyValueStoreMap =
                mapByName(loadedKeyValueStoreMap, KeyValueStore::getName);
            keyValueStoreMap.putAll(tmpKeyValueStoreMap);

            // temporary storPool map
            Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> tmpStorPoolMapForLayers =
                new TreeMap<>();
            for (Entry<StorPool, StorPool.InitMaps> entry : loadedStorPools.entrySet())
            {
                StorPool storPool = entry.getKey();
                tmpStorPoolMapForLayers.put(
                    new PairNonNull<>(
                        storPool.getNode().getName(),
                        storPool.getName()
                    ),
                    new PairNonNull<>(
                        storPool,
                        entry.getValue()
                    )
                );
            }

            nodesMap.putAll(tmpNodesMap);
            rscDfnMap.putAll(tmpRscDfnMap);
            rscGrpMap.putAll(tmpRscGroups);
            storPoolDfnMap.putAll(tmpStorPoolDfnMap);
            freeSpaceMgrMap.putAll(tmpFreeSpaceMgrMap);
            extFileMap.putAll(tmpExtFileMap);
            remoteMap.putAll(tmpRemoteMap);
            scheduleMap.putAll(tmpScheduleMap);


            // load layer objects
            loadLayerObects(
                tmpRscDfnMap,
                tmpRscMap,
                tmpSnapshotDfnMap,
                tmpSnapshotMap,
                tmpStorPoolMapForLayers
            );

            // load external names
            for (ResourceDefinition rscDfn : tmpRscDfnMap.values())
            {
                final byte[] extName = rscDfn.getExternalName();
                if (extName != null)
                {
                    final ResourceDefinition otherRscDfn = rscDfnMapExtName.putIfAbsent(extName, rscDfn);
                    if (otherRscDfn != null)
                    {
                        throw new DatabaseException(
                            "Duplicate external name, resource definitions: " +
                                rscDfn.getName() + ", " + otherRscDfn.getName()
                        );
                    }
                }
            }

            AbsRscLayerHelper.databaseLoadingFinished();

            propsDriver.clearCache();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("dbCtx has not enough privileges", exc);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded props key", exc);
        }
        catch (LinStorException exc)
        {
            throw new ImplementationError("Unknown error during loading data from DB", exc);
        }
        catch (InvalidNameException | ValueOutOfRangeException | InvalidIpAddressException | MdException exc)
        {
            throw new ImplementationError("Unknown error during loading data from DB", exc);
        }
        finally
        {
            storPoolResolveHelper.setEnableChecks(true);
        }
    }

    public static <NAME, DATA, IN_DATA extends DATA> TreeMap<NAME, DATA> mapByName(
        Map<IN_DATA, ?> map,
        Function<IN_DATA, NAME> nameMapper
    )
    {
        TreeMap<NAME, DATA> ret = new TreeMap<>();
        for (IN_DATA data : map.keySet())
        {
            ret.put(nameMapper.apply(data), data);
        }
        return ret;
    }

    private void loadLayerObects(
        Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef,
        Map<Pair<NodeName, ResourceName>, Resource> tmpRscMapRef,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> tmpSnapDfnMapRef,
        Map<Triple<NodeName, ResourceName, SnapshotName>, Snapshot> tmpSnapMapRef,
        Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> tmpStorPoolMapWithInitMapsRef
    )
        throws DatabaseException, AccessDeniedException, ImplementationError, InvalidNameException,
        ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        // load RscDfnLayerObjects and VlmDfnLayerObjects
        // no *DfnLayerObjects for nvme
        // no *DfnLayerObjects for luks

        ParentObjects parentObjects = buildLayerParentObjects(
            tmpRscDfnMapRef,
            tmpRscMapRef,
            tmpSnapDfnMapRef,
            tmpSnapMapRef,
            tmpStorPoolMapWithInitMapsRef
        );
        for (ControllerLayerRscDatabaseDriver driver : layerDriversMap.values())
        {
            driver.cacheAll(parentObjects);
        }

        Set<Resource> resourcesWithLayerData = loadLayerData(
            parentObjects,
            tmpStorPoolMapWithInitMapsRef,
            rli ->
            {
                // snamshotName != null means this is a snapshot, not a resource.
                return rli.getSnapName() != null ?
                    null :
                    tmpRscDfnMapRef.get(rli.getResourceName()).getResource(dbCtx, rli.getNodeName());
            }
        );

        // this needs to be called to load the snapshot-layerdata and save it into the respective snapshot. The return
        // value can be safely ignored
        loadLayerData(
            parentObjects,
            tmpStorPoolMapWithInitMapsRef,
            rli ->
            {
                Snapshot snap = null;
                if (rli.getSnapName() != null)
                {
                    SnapshotDefinition snapshotDefinition = tmpSnapDfnMapRef.get(
                        new Pair<>(
                            rli.getResourceName(),
                            rli.getSnapName()
                        )
                    );
                    if (snapshotDefinition != null)
                    {
                        snap = snapshotDefinition.getSnapshot(dbCtx, rli.getNodeName());
                    }
                }
                return snap;
            }
        );

        /**
         * This needs to be done AFTER we have already loaded all layerRscData (for both, Resources and Snapshots).
         * Otherwise, if only the layerRscData for Resources are loaded, loading the layerVlmData for Snapshots would
         * not find their corresponding layerRscData
         */
        for (ControllerLayerRscDatabaseDriver driver : layerDriversMap.values())
        {
            driver.loadAllLayerVlmData();
        }

        for (ControllerLayerRscDatabaseDriver driver : layerDriversMap.values())
        {
            driver.clearLoadingCaches();
        }

        CtrlRscLayerDataFactory rscLayerDataHelper = ctrlRscLayerDataHelper.get();
        for (Resource rsc : resourcesWithLayerData)
        {
            LayerPayload payload = new LayerPayload();
            // initialize all non-persisted, but later serialized variables
            List<DeviceLayerKind> layerStack = LayerUtils.getLayerStack(rsc, dbCtx);
            rscLayerDataHelper.ensureStackDataExists(rsc, layerStack, payload);
        }
    }

    private ParentObjects buildLayerParentObjects(
        Map<ResourceName, ResourceDefinition> tmpRscDfnMapRef,
        Map<Pair<NodeName, ResourceName>, Resource> tmpRscMapRef,
        Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> tmpSnapDfnMapRef,
        Map<Triple<NodeName, ResourceName, SnapshotName>, Snapshot> tmpSnapMapRef,
        Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> tmpStorPoolMapWithInitMapsRef
    )
        throws DatabaseException
    {
        List<AbsRscLayerObject<?>> rscLayerObjPojoList = layerRscIdDriver.loadAllAsList(null);

        Map<Integer, AbsRscLayerObject<?>> loadingLayerRscObjectsById = rscLayerObjPojoList.stream()
            .collect(
                Collectors.toMap(
                    rlo -> rlo.getRscLayerId(),
                    Function.identity(),
                    throwingMerger(),
                    HashMap::new
                )
            );

        return new ParentObjects(
            loadingLayerRscObjectsById,
            tmpRscMapRef,
            tmpRscDfnMapRef,
            tmpSnapMapRef,
            tmpSnapDfnMapRef,
            tmpStorPoolMapWithInitMapsRef
        );
    }

    private <RSC extends AbsResource<RSC>> Set<RSC> loadLayerData(
        ParentObjects parentObjectsRef,
        Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> tmpStorPoolMapWithInitMapsRef,
        ExceptionThrowingFunction<AbsRscLayerObject<?>, RSC, AccessDeniedException> getter
    )
        throws DatabaseException, AccessDeniedException, ImplementationError, InvalidNameException,
        ValueOutOfRangeException, InvalidIpAddressException, MdException
    {
        Set<Integer> parentIds = null;
        boolean loadNext = true;
        Map<Integer, Pair<AbsRscLayerObject<RSC>, Set<AbsRscLayerObject<RSC>>>> rscLayerObjectChildren;
        rscLayerObjectChildren = new HashMap<>();

        List<AbsRscLayerObject<?>> allRLOPojoList = parentObjectsRef.getAllRloPojoList();

        Set<RSC> resourcesWithLayerData = new HashSet<>();
        while (loadNext)
        {
            // we need to load in a top-down fashion.
            List<AbsRscLayerObject<?>> nextRscLayerObjPojoToLoad = nextRscLayerObjPojosToLoad(
                allRLOPojoList,
                parentIds
            );
            loadNext = !nextRscLayerObjPojoToLoad.isEmpty();

            parentIds = new HashSet<>();
            for (AbsRscLayerObject<?> rlo : nextRscLayerObjPojoToLoad)
            {
                Pair<? extends AbsRscLayerObject<RSC>, Set<AbsRscLayerObject<RSC>>> rscLayerObjectPair;
                RSC rsc = getter.accept(rlo);

                if (rsc != null)
                {
                    // rsc will be null if the getter for a snapshot finds a resource and vice versa
                    resourcesWithLayerData.add(rsc);

                    AbsRscLayerObject<RSC> parent = null;
                    Set<AbsRscLayerObject<RSC>> currentRscLayerDatasChildren = null;
                    if (rlo.getParent() != null)
                    {
                        Pair<AbsRscLayerObject<RSC>, Set<AbsRscLayerObject<RSC>>> pair = rscLayerObjectChildren
                            .get(rlo.getParent().getRscLayerId());

                        parent = pair.objA;
                        currentRscLayerDatasChildren = pair.objB;
                    }
                    try
                    {
                        ControllerLayerRscDatabaseDriver driver = layerDriversMap.get(rlo.getLayerKind());
                        if (driver == null)
                        {
                            throw new ImplementationError(
                                "No driver found for device kind '" + rlo.getLayerKind() + "'"
                            );
                        }
                        rscLayerObjectPair = driver.load(rsc, rlo.getRscLayerId());
                    }
                    catch (LinStorDBRuntimeException exc)
                    {
                        throw exc;
                    }
                    catch (RuntimeException runtimeExc)
                    {
                        String objDescr;
                        if (rsc instanceof Resource)
                        {
                            objDescr = String.format(
                                "resource '%s' on node '%s'",
                                rsc.getNode().getName().displayValue,
                                rsc.getResourceDefinition().getName().displayValue
                            );
                        }
                        else
                        {
                            objDescr = String.format(
                                "snapshot '%s' of resource '%s' on node '%s'",
                                ((Snapshot) rsc).getSnapshotName().displayValue,
                                rsc.getNode().getName().displayValue,
                                rsc.getResourceDefinition().getName().displayValue
                            );
                        }
                        throw new LinStorDBRuntimeException(
                            String.format(
                                "Error occured while loading %s, layer id: %d",
                                objDescr,
                                rlo.getRscLayerId()
                            ),
                            runtimeExc
                        );
                    }
                    AbsRscLayerObject<RSC> rscLayerObject = rscLayerObjectPair.objA;
                    rscLayerObjectChildren
                        .put(rlo.getRscLayerId(), new Pair<>(rscLayerObject, rscLayerObjectPair.objB));
                    if (parent == null)
                    {
                        rsc.setLayerData(dbCtx, rscLayerObject);
                    }
                    else
                    {
                        currentRscLayerDatasChildren.add(rscLayerObject);
                    }

                    // rli will be the parent for the next iteration
                    parentIds.add(rlo.getRscLayerId());
                }
            }
        }

        return resourcesWithLayerData;
    }

    private List<AbsRscLayerObject<?>> nextRscLayerObjPojosToLoad(
        List<AbsRscLayerObject<?>> rscLayerInfoListRef,
        @Nullable Set<Integer> ids
    )
    {
        List<AbsRscLayerObject<?>> ret;
        if (ids == null)
        {
            // root rscLayerObjects
            ret = rscLayerInfoListRef.stream().filter(rlo -> rlo.getParent() == null).collect(Collectors.toList());
        }
        else
        {
            ret = rscLayerInfoListRef.stream()
                .filter(rlo -> rlo.getParent() != null && ids.contains(rlo.getParent().getRscLayerId()))
                .collect(Collectors.toList());
        }
        return ret;
    }

    @Override
    public ServiceName getDefaultServiceInstanceName()
    {
        return DFLT_SERVICE_INSTANCE_NAME;
    }

    public static List<String> asStrList(Collection<DeviceLayerKind> layerStackRef)
    {
        List<String> ret = new ArrayList<>();
        for (DeviceLayerKind kind : layerStackRef)
        {
            ret.add(kind.name());
        }
        return ret;
    }

    public static List<DeviceLayerKind> asDevLayerKindList(Collection<String> strList)
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        if (strList != null)
        {
            for (String str : strList)
            {
                ret.add(DeviceLayerKind.valueOf(str));
            }
        }
        return ret;
    }

    public static List<DeviceProviderKind> asDevLayerProviderList(Collection<String> strList)
    {
        List<DeviceProviderKind> ret = new ArrayList<>();
        if (strList != null)
        {
            for (String str : strList)
            {
                ret.add(DeviceProviderKind.valueOf(str));
            }
        }
        return ret;
    }

    public static void handleAccessDeniedException(AccessDeniedException accDeniedExc)
        throws ImplementationError
    {
        throw new ImplementationError(
            "Database's access context has insufficient permissions",
            accDeniedExc
        );
    }

    private static <T> BinaryOperator<T> throwingMerger()
    {
        return (key, value) ->
        {
            throw new ImplementationError("At least two objects have the same name.\n" +
                "That should have caused an sql exception when inserting. Key: '" + key + "'",
                null
            );
        };
    }
}
