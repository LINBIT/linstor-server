package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.PlatformStlt;
import com.linbit.extproc.ChildProcessHandler;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.pojo.StltRemotePojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.prop.WhitelistPropsReconfigurator;
import com.linbit.linstor.api.protobuf.FullSync;
import com.linbit.linstor.api.protobuf.FullSync.FullSyncResult;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.core.ApplicationLifecycleManager;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.UpdateMonitor;
import com.linbit.linstor.core.apicallhandler.StltStorPoolApiCallHandler.ChangedData;
import com.linbit.linstor.core.apicallhandler.satellite.authentication.AuthenticationResult;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.migration.StltMigrationHandler;
import com.linbit.linstor.core.migration.StltMigrationHandler.StltMigrationResult;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.Watch;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventPublisher;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdResource;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdStateTracker;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdVolume;
import com.linbit.linstor.layer.storage.DeviceProvider;
import com.linbit.linstor.layer.storage.DeviceProviderMapper;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.common.StltConfigOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.event.Level;

@Singleton
public class StltApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final StltConfig stltCfg;

    private final ControllerPeerConnector controllerPeerConnector;
    private final UpdateMonitor updateMonitor;
    private final DeviceManager deviceManager;
    private final ApplicationLifecycleManager applicationLifecycleManager;

    private final StltNodeApiCallHandler nodeHandler;
    private final StltRscDfnApiCallHandler rscDfnHandler;
    private final StltRscApiCallHandler rscHandler;
    private final StltStorPoolApiCallHandler storPoolHandler;
    private final StltSnapshotApiCallHandler snapshotHandler;
    private final StltExternalFilesApiCallHandler extFilesHandler;
    private final StltRemoteApiCallHandler remoteHandler;
    private final StltExtToolsChecker stltExtToolsChecker;

    private final CtrlStltSerializer interComSerializer;

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ReadWriteLock extFileMapLock;
    private final ReadWriteLock remoteMapLock;
    private final CoreModule.NodesMap nodesMap;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    private final TreeMap<Long, ApplyData> dataToApply;

    private final Provider<TransactionMgr> transMgrProvider;
    private final StltSecurityObjects stltSecObj;
    private final StltCryptApiCallHelper vlmDfnHandler;
    private final Props stltConf;
    private final EventBroker eventBroker;
    private final DeviceProviderMapper deviceProviderMapper;

    private WhitelistPropsReconfigurator whiteListPropsReconfigurator;

    private final Provider<Long> apiCallId;
    private DrbdStateTracker drbdStateTracker;
    private DrbdEventPublisher drbdEventPublisher;
    private final BackupShippingMgr backupShippingMgr;
    private final StltApiCallHandlerUtils stltApiCallHandlerUtils;
    private final StltMigrationHandler stltMigrationHandler;

    private final PlatformStlt platformStlt;

    @Inject
    public StltApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        StltConfig stltCfgRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        UpdateMonitor updateMonitorRef,
        DeviceManager deviceManagerRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        StltNodeApiCallHandler nodeHandlerRef,
        StltRscDfnApiCallHandler rscDfnHandlerRef,
        StltRscApiCallHandler rscHandlerRef,
        StltStorPoolApiCallHandler storPoolHandlerRef,
        StltSnapshotApiCallHandler snapshotHandlerRef,
        StltExternalFilesApiCallHandler extFilesHandlerRef,
        StltRemoteApiCallHandler remoteHandlerRef,
        StltExtToolsChecker stltExtToolsCheckerRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.EXT_FILE_MAP_LOCK) ReadWriteLock extFileMapLockRef,
        @Named(CoreModule.REMOTE_MAP_LOCK) ReadWriteLock remoteMapLockRef,
        @Named(LinStor.SATELLITE_PROPS) Props satellitePropsRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects stltSecObjRef,
        StltCryptApiCallHelper vlmDfnHandlerRef,
        EventBroker eventBrokerRef,
        WhitelistPropsReconfigurator whiteListPropsReconfiguratorRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        DrbdStateTracker drbdStateTrackerRef,
        DrbdEventPublisher drbdEventPublisherRef,
        DeviceProviderMapper deviceProviderMapperRef,
        BackupShippingMgr backupShippingMgrRef,
        StltApiCallHandlerUtils stltApiCallHandlerUtilsRef,
        PlatformStlt platformStltRef,
        StltMigrationHandler stltMigrationHandlerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        stltCfg = stltCfgRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        updateMonitor = updateMonitorRef;
        deviceManager = deviceManagerRef;
        applicationLifecycleManager = applicationLifecycleManagerRef;
        nodeHandler = nodeHandlerRef;
        rscDfnHandler = rscDfnHandlerRef;
        rscHandler = rscHandlerRef;
        storPoolHandler = storPoolHandlerRef;
        snapshotHandler = snapshotHandlerRef;
        extFilesHandler = extFilesHandlerRef;
        remoteHandler = remoteHandlerRef;
        stltExtToolsChecker = stltExtToolsCheckerRef;
        interComSerializer = interComSerializerRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        extFileMapLock = extFileMapLockRef;
        remoteMapLock = remoteMapLockRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        transMgrProvider = transMgrProviderRef;
        stltSecObj = stltSecObjRef;
        vlmDfnHandler = vlmDfnHandlerRef;
        stltConf = satellitePropsRef;
        eventBroker = eventBrokerRef;
        whiteListPropsReconfigurator = whiteListPropsReconfiguratorRef;
        apiCallId = apiCallIdRef;
        drbdStateTracker = drbdStateTrackerRef;
        drbdEventPublisher = drbdEventPublisherRef;
        deviceProviderMapper = deviceProviderMapperRef;
        backupShippingMgr = backupShippingMgrRef;
        stltApiCallHandlerUtils = stltApiCallHandlerUtilsRef;
        platformStlt = platformStltRef;
        stltMigrationHandler = stltMigrationHandlerRef;

        dataToApply = new TreeMap<>();
    }

    public AuthenticationResult authenticate(
        UUID nodeUuid,
        String nodeName,
        Peer controllerPeer,
        @Nullable UUID ctrlUuid
    )
    {
        AuthenticationResult authResult;

        synchronized (dataToApply)
        {
            dataToApply.clear(); // controller should not have sent us anything before the authentication.
            // that means, everything in this map is out-dated data + we should receive a full sync next.
        }

        // re-cache external tools before calling "setControllerPeer", since that also initializes the peer's
        // ExtToosManager with the cached values of the stltExtToolChecker
        Collection<ExtToolsInfo> extToolsInfoList = stltExtToolsChecker.getExternalTools(true).values();
        controllerPeerConnector.setControllerPeer(
            ctrlUuid,
            controllerPeer,
            nodeUuid,
            nodeName
        );

        // FIXME In the absence of any means of identification, assume the identity of the privileged API context
        // for the peer.
        AccessContext curCtx = controllerPeer.getAccessContext();
        try
        {
            AccessContext newCtx = apiCtx.impersonate(
                apiCtx.subjectId, curCtx.subjectRole, curCtx.subjectDomain
            );
            controllerPeer.setAccessContext(apiCtx, newCtx);
        }
        catch (AccessDeniedException accExc)
        {
            errorReporter.reportError(
                Level.ERROR,
                new ImplementationError(
                    "Creation of an access context for a Controller by the " +
                    apiCtx.subjectRole.name.displayValue + " role failed",
                    accExc
                )
            );
        }
        try
        {
            // this will be cleared and re-set in fullsync. This is just for safety so that this property
            // is always set
            stltConf.setProp(LinStor.KEY_NODE_NAME, nodeName);
            transMgrProvider.get().commit();

            authResult = new AuthenticationResult(
                extToolsInfoList,
                ApiCallRcImpl.singletonApiCallRc(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.CREATED | ApiConsts.MASK_NODE,
                        "successfully authenticated"
                    )
                )
            );
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidValueException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        errorReporter.logInfo("Controller connected and authenticated (" + controllerPeer.getId() + ")");

        return authResult;
    }

    public FullSync.FullSyncResult applyFullSync(
        Map<String, String> satelliteProps,
        Set<NodePojo> nodes,
        Set<StorPoolPojo> storPools,
        Set<RscPojo> resources,
        Set<SnapshotPojo> snapshots,
        Set<ExternalFilePojo> extFilesRef,
        Set<S3RemotePojo> s3remotes,
        Set<EbsRemotePojo> ebsRemotes,
        long fullSyncId,
        byte[] cryptKey,
        byte[] cryptHash,
        byte[] cryptSalt,
        byte[] encCryptKey
    )
    {
        FullSync.FullSyncStatus success;
        Map<String, String> stltPropsToAdd = new HashMap<>();
        Set<String> stltPropKeysToDelete = new HashSet<>();
        Set<String> stltPropNamespacesToDelete = new HashSet<>();

        try (
            LockGuard ls = LockGuard.createLocked(
                reconfigurationLock.writeLock(),
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock(),
                storPoolDfnMapLock.writeLock(),
                remoteMapLock.writeLock()
            )
        )
        {
            if (updateMonitor.getCurrentFullSyncId() == fullSyncId)
            {
                // only apply this fullSync if it is newer than the last one

                // first delete all kind of stuff
                stltApiCallHandlerUtils.clearCoreMaps();
                stltApiCallHandlerUtils.clearCaches();
                deleteOldResFiles(); // new res files should be (re-)generated in the next devMgrCycle

                // now start to (re-) create linstor objects received from controller
                for (NodePojo node : nodes)
                {
                    Node curNode = nodeHandler.applyChanges(node);
                    if (curNode != null)
                    {
                        nodesMap.put(curNode.getName(), curNode);
                    }
                }
                controllerPeerConnector.setControllerPeerToCurrentLocalNode();

                doApplyControllerChanges(satelliteProps, true);

                /*
                 * At least openflex stor pools need the properties of the localnode in order to
                 * query freeSpace (which is triggered by storPoolHandler.applyChanges)
                 */
                deviceManager.applyChangedNodeProps(controllerPeerConnector.getLocalNode().getProps(apiCtx));

                for (StorPoolPojo storPool : storPools)
                {
                    ChangedData appliedChanges = storPoolHandler.applyChanges(storPool);
                    StorPoolDefinition storPoolDfnToRegister = appliedChanges.storPoolDfnToRegister;
                    if (storPoolDfnToRegister != null)
                    {
                        storPoolDfnMap.put(
                            storPoolDfnToRegister.getName(),
                            storPoolDfnToRegister
                        );
                    }
                }

                for (RscPojo rsc : resources)
                {
                    /*
                     * usually the controller already runs checks before sending the data to the satellite, which is why
                     * we only need this check during a fullsync, not during a regular applyResource.
                     *
                     * The external tools might have changed since the last connection (i.e. kernel upgrade with
                     * unintentional DRBD downgrade)
                     */
                    checkLayersForExtToolsSupport(rsc.getLayerData());
                    rscHandler.applyChanges(rsc);
                }

                for (SnapshotPojo snapshot : snapshots)
                {
                    snapshotHandler.applyChanges(snapshot);
                }

                for (ExternalFilePojo extFilePojo : extFilesRef)
                {
                    extFilesHandler.applyChanges(extFilePojo);
                }

                for (S3RemotePojo s3remote : s3remotes)
                {
                    remoteHandler.applyChangesS3(s3remote);
                }
                for (EbsRemotePojo ebsRemote : ebsRemotes)
                {
                    remoteHandler.applyChangesEbs(ebsRemote);
                }

                transMgrProvider.get().commit();

                for (NodePojo node : nodes)
                {
                    errorReporter.logTrace("Node '" + node.getName() + "' received from Controller.");
                }
                for (StorPoolPojo storPool : storPools)
                {
                    errorReporter.logTrace(
                        "StorPool '" + storPool.getStorPoolName() + "' received from Controller."
                    );
                }
                for (RscPojo rsc : resources)
                {
                    errorReporter.logTrace("Resource '" + rsc.getName() + "' received.");
                }
                for (SnapshotPojo snapshot : snapshots)
                {
                    errorReporter.logTrace("Snapshot '" + snapshot.getSnaphotDfn() + "' received.");
                }
                for (ExternalFilePojo extFilePojo : extFilesRef)
                {
                    errorReporter.logTrace("External file '%s' received.", extFilePojo.getFileName());
                }
                for (S3RemotePojo s3remote : s3remotes)
                {
                    errorReporter.logTrace("Remote '" + s3remote.getRemoteName() + "' received.");
                }
                for (EbsRemotePojo ebsRemote : ebsRemotes)
                {
                    errorReporter.logTrace("Remote '" + ebsRemote.getRemoteName() + "' received.");
                }
                errorReporter.logTrace("Full sync with controller finished");

                // Atomically notify the DeviceManager to check all resources
                Node localNode = controllerPeerConnector.getLocalNode();
                if (localNode != null)
                {
                    deviceManager.fullSyncApplied(localNode);
                }
                else
                {
                    errorReporter.logWarning(
                        "No node object that represents this satellite was received from the controller"
                    );
                }

                if (cryptKey != null && cryptKey.length > 0)
                {
                    stltSecObj.setCryptKey(cryptKey, cryptHash, cryptSalt, encCryptKey);

                    vlmDfnHandler.decryptVolumesAndDrives(true);
                }

                whiteListPropsReconfigurator.reconfigure();

                updateMonitor.setFullSyncApplied();

                errorReporter.logTrace("FullSync registered");

                // There are no explicit controller - satellite watches.
                // FullSync implicitly creates a watch for all events.
                createWatchForPeer();

                for (RscPojo rsc : resources)
                {
                    checkForAlreadyKnownResources(rsc, true);
                }

                StltMigrationResult migrationResult = stltMigrationHandler.migrate();
                success = migrationResult.status;
                migrationResult.copyInto(stltPropsToAdd, stltPropKeysToDelete, stltPropNamespacesToDelete);
            }
            else
            {
                errorReporter.logWarning(
                    "Ignored an incoming but outdated fullsync (%d, expected: %d)",
                    fullSyncId,
                    updateMonitor.getCurrentFullSyncId()
                );
                success = FullSync.FullSyncStatus.SUCCESS;
            }
        }
        catch (Exception | ImplementationError exc)
        {
            if (exc instanceof MissingRequiredExtToolsStorageException)
            {
                success = FullSync.FullSyncStatus.FAIL_MISSING_REQUIRED_EXT_TOOLS;
            }
            else
            {
                success = FullSync.FullSyncStatus.FAIL_UNKNOWN;
            }
            errorReporter.reportError(exc);

            // this method returning false should trigger a stlt->ctrl message that the full sync failed

            // sending that message should tell the controller to not send us any further data, as
            // updates would be based on an invalid fullSync, and receiving this fullSync again
            // would most likely cause the same exception as now.

            // however, in order to avoid implementation errors of the controller, we additionally
            // increase the fullSyncId but not telling the controller about it.
            // even if the controller still sends us data, we will ignore them as they will look like
            // "out-dated" data.
            // when recreating the connection, and the controller is positive to send us an authentication
            // message, we will again increase the fullSyncId and expect the fullSync from the controller.

            // in other words: if this exception happens, either the controller or this satellite has
            // to drop the connection (e.g. restart) in order to re-enable applying fullSyncs.
            updateMonitor.getNextFullSyncId();
        }
        return new FullSyncResult(success, stltPropsToAdd, stltPropKeysToDelete, stltPropNamespacesToDelete);
    }

    private void checkForAlreadyKnownResources(RscPojo rsc, boolean forceEventTrigger)
    {
        /*
         *  in rare cases (e.g. migration) it is possible that a DRBD-resource already
         *  exists and we receive "create drbd resource" from the "events2"-stream
         *  before the controller tells us about that resource.
         *
         *  In this case we have to update the corresponding DrbdResources that from now
         *  on we know them and are interested in the changes, and send the controller
         *  the initial "resource created" event.
         */

        /*
         * In cases where a resource has nothing to do with DRBD (or is not deployed yet)
         * the drbdStateTracker will simply return null and this block results in a no-op.
         *
         * If later drbd fires the "create drbd resource" in its "events2" stream, there is
         * also a check if we already know this resource, so that way is also covered.
         */

        /*
         * The event should be sent to ctrl during FullSync as well as for newly created DRBD resource,
         * but NOT when adjusting existing DRBD resources.
         */

        boolean triggerEvent = forceEventTrigger;
        DrbdResource drbdResource = drbdStateTracker.getResource(rsc.getName());
        if (drbdResource != null && !drbdResource.isKnownByLinstor())
        {
            drbdResource.setKnownByLinstor(true);
            triggerEvent = true;
        }
        if (drbdResource != null && drbdResource.isKnownByLinstor() && triggerEvent)
        {
            drbdEventPublisher.resourceCreated(drbdResource);

            Iterator<DrbdVolume> itVlm = drbdResource.iterateVolumes();
            while (itVlm.hasNext())
            {
                DrbdVolume drbdVlm = itVlm.next();
                drbdEventPublisher.volumeCreated(drbdResource, null, drbdVlm);
            }

            // trigger connection volumes (replication states)
            for (var conn : drbdResource.getConnectionsMap().values())
            {
                Iterator<DrbdVolume> itConnVlm = conn.iterateVolumes();
                while (itConnVlm.hasNext())
                {
                    DrbdVolume drbdVlm = itConnVlm.next();
                    drbdEventPublisher.volumeCreated(drbdResource, conn, drbdVlm);
                }
            }
        }
    }

    private void createWatchForPeer()
    {
        Peer controllerPeer = controllerPeerConnector.getControllerPeer();
        eventBroker.createWatch(
            controllerPeer,
            new Watch(
                UUID.randomUUID(),
                controllerPeer.getId(),
                0,
                EventIdentifier.global(null)
        ));
    }

    public void applyControllerChanges(
        Map<String, String> satelliteProps,
        long fullSyncId,
        long updateId
    )
    {
        applyChangedData(new ApplyControllerData(satelliteProps, fullSyncId, updateId));
    }

    private void doApplyControllerChanges(Map<String, String> satelliteProps, boolean runSpUpdatesRef)
    {
        try
        {
            stltConf.map().putAll(satelliteProps);
            stltConf.keySet().retainAll(satelliteProps.keySet());

            // local nodename needed by openflex driver
            stltConf.setProp(LinStor.KEY_NODE_NAME, controllerPeerConnector.getLocalNodeName().displayValue);

            String extCmdWaitToStr = stltConf.getProp(ApiConsts.KEY_EXT_CMD_WAIT_TO);
            if (extCmdWaitToStr != null)
            {
                ChildProcessHandler.dfltWaitTimeout = Long.parseLong(extCmdWaitToStr);
            }

            LvmUtils.updateCacheTime(stltConf, controllerPeerConnector.getLocalNode().getProps(apiCtx));

            transMgrProvider.get().commit();

            reconfigureAllStorageDrivers(runSpUpdatesRef);

            Set<ResourceName> slctRsc = new TreeSet<>();
            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                slctRsc.add(curRscDfn.getName());
            }

            deviceManager.controllerUpdateApplied(slctRsc);
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void checkLayersForExtToolsSupport(RscLayerDataApi layerData) throws MissingRequiredExtToolsStorageException
    {
        DeviceLayerKind layerKind = layerData.getLayerKind();
        for (ExtTools reqExtTool : layerKind.getExtToolDependencies())
        {
            if (!stltExtToolsChecker.getExternalTools(false).get(reqExtTool).isSupported())
            {
                throw new MissingRequiredExtToolsStorageException(
                    "Received a resource that requires " + reqExtTool.name() +
                        " but that external tool is not supported on this satellite"
                    );
            }
        }

        // recursive check for all children
        for (RscLayerDataApi childData : layerData.getChildren())
        {
            checkLayersForExtToolsSupport(childData);
        }
    }

    private void reconfigureAllStorageDrivers(boolean runSpUpdatesRef)
    {
        try
        {
            Node localNode = controllerPeerConnector.getLocalNode();
            if (localNode != null)
            {
                for (StorPoolDefinition spd : storPoolDfnMap.values())
                {
                    try
                    {
                        @Nullable StorPool sp = spd.getStorPool(apiCtx, controllerPeerConnector.getLocalNodeName());
                        // storage pool probably not deployed on this node
                        if (sp != null)
                        {
                            DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderBy(sp);
                            if (runSpUpdatesRef)
                            {
                                @Nullable LocalPropsChangePojo pojo = deviceProvider.update(sp);
                                if (pojo != null)
                                {
                                    controllerPeerConnector.getControllerPeer()
                                    .sendMessage(
                                        interComSerializer
                                                .onewayBuilder(InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT)
                                                .updateLocalProps(pojo)
                                                .build(),
                                        InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT
                                        );
                                }
                            }
                            deviceProvider.checkConfig(sp);
                        }
                    }
                    catch (StorageException exc)
                    {
                        errorReporter.reportError(exc);
                    }
                    catch (DatabaseException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("Privileged API context has insufficient privileges");
        }
    }

    public void applyNodeChanges(NodePojo nodePojo)
    {
        applyChangedData(new ApplyNode(nodePojo));
    }

    public void applyDeletedNodeChange(
        String nodeName,
        long fullSyncId,
        long updateId
    )
    {
        applyChangedData(new ApplyNode(nodeName, fullSyncId, updateId));
    }

    public void applyResourceChanges(RscPojo rscRawData)
    {
        applyChangedData(new ApplyRscData(rscRawData));
    }

    public void applyDeletedResourceChange(
        String rscNameStr,
        long fullSyncId,
        long updateId
    )
    {
        applyChangedData(new ApplyRscData(rscNameStr, fullSyncId, updateId));
    }

    public void applyStorPoolChanges(StorPoolPojo storPoolRaw)
    {
        applyChangedData(new ApplyStorPool(storPoolRaw));
    }

    public void applyDeletedStorPoolChange(
        String storPoolNameStr,
        long fullSyncId,
        long updateId
    )
    {
        applyChangedData(new ApplyStorPool(storPoolNameStr, fullSyncId, updateId));
    }

    public void applySnapshotChanges(SnapshotPojo snapshotRaw)
    {
        applyChangedData(new ApplySnapshot(snapshotRaw));
    }

    public void applyEndedSnapshotChange(String rscName, String snapshotName, long fullSyncId, long updateId)
    {
        applyChangedData(new ApplyEndedSnapshot(rscName, snapshotName, fullSyncId, updateId));
    }

    public void applyExternalFileChanges(ExternalFilePojo extFilePojoRef)
    {
        applyChangedData(new ApplyExternalFile(extFilePojoRef, false));
    }

    public void applyDeletedExternalFileChanges(ExternalFilePojo extFilePojoRef)
    {
        applyChangedData(new ApplyExternalFile(extFilePojoRef, true));
    }

    public void applyS3RemoteChanges(S3RemotePojo s3remotePojoRef)
    {
        applyChangedData(new ApplyS3Remote(s3remotePojoRef));
    }

    public void applyStltRemoteChanges(StltRemotePojo stltRemotePojoRef)
    {
        applyChangedData(new ApplyStltRemote(stltRemotePojoRef, false));
    }

    public void applyEbsRemoteChanges(EbsRemotePojo ebsRemotePojoRef)
    {
        applyChangedData(new ApplyEbsRemote(ebsRemotePojoRef));
    }

    public void applyDeletedRemoteChanges(String remoteNameRef, long fullSyncId, long updateId)
    {
        applyChangedData(new ApplyRemoteDelete(remoteNameRef, fullSyncId, updateId));
    }

    public void setCryptKey(byte[] key, byte[] hash, byte[] salt, byte[] encKey, long fullSyncId, long updateId)
    {
        applyChangedData(new ApplyCryptKey(key, hash, salt, encKey, fullSyncId, updateId));
    }

    private void applyChangedData(ApplyData data)
    {
        synchronized (dataToApply)
        {
            if (data.getFullSyncId() == updateMonitor.getCurrentFullSyncId())
            {
                try
                {
                    ApplyData overriddenData = dataToApply.put(data.getUpdateId(), data);
                    if (overriddenData != null)
                    {
                        errorReporter.reportError(
                            new ImplementationError(
                                "We have overridden data which we did not update yet.",
                                null
                            )
                        );
                        // critical error. shutdown and fix this implementation error
                        applicationLifecycleManager.shutdown(apiCtx);
                    }

                    Entry<Long, ApplyData> nextEntry;
                    nextEntry = dataToApply.firstEntry();
                    while (
                        nextEntry != null &&
                        nextEntry.getKey() == updateMonitor.getCurrentAwaitedUpdateId()
                    )
                    {
                        errorReporter.logTrace("Applying update " + nextEntry.getKey());

                        ApplyData applyData = nextEntry.getValue();
                        try (
                            LockGuard ls = LockGuard.createLocked(
                                applyData.needReconfigurationWriteLock() ?
                                    reconfigurationLock.writeLock() : reconfigurationLock.readLock()
                            )
                        )
                        {
                            applyData.applyChange();
                        }
                        dataToApply.remove(nextEntry.getKey());
                        updateMonitor.awaitedUpdateApplied();

                        nextEntry = dataToApply.firstEntry();
                    }
                    for (Entry<Long, ApplyData> remainingDataToApply : dataToApply.entrySet())
                    {
                        errorReporter.logDebug("Update " + remainingDataToApply.getKey() +
                            " queued until update " + updateMonitor.getCurrentAwaitedUpdateId() + " received");
                    }
                }
                catch (ImplementationError | Exception exc)
                {
                    errorReporter.reportError(exc);
                    try
                    {
                        controllerPeerConnector.getLocalNode().getPeer(apiCtx).closeConnection();
                        // there is nothing else we can safely do.
                        // skipping the update might cause data-corruption
                        // not skipping will queue the new data packets but will not apply those as the
                        // awaitedUpdateId will never increment.
                    }
                    catch (AccessDeniedException exc1)
                    {
                        errorReporter.reportError(new ImplementationError(exc));
                    }
                }
            }
            else
            {
                errorReporter.logWarning("Ignoring received outdated update. ");
            }
        }
    }

    public void handlePrimaryResource(
        String rscNameStr,
        UUID rscUuid
    )
    {
        try (LockGuard ls = LockGuard.createLocked(rscDfnMapLock.writeLock()))
        {
            rscDfnHandler.primaryResource(rscNameStr, rscUuid);
        }

    }

    public void backupShippingFinished(String rscName, String snapName)
    {
        backupShippingMgr.removeSnapFromStartedShipments(rscName, snapName);
    }

    public byte[] listErrorReports(
        final Set<String> nodes,
        boolean withContent,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset
    )
    {
        ErrorReportResult errorReportResult = errorReporter.listReports(
            withContent,
            since,
            to,
            ids,
            limit,
            offset
        );

        return interComSerializer.answerBuilder(ApiConsts.API_LST_ERROR_REPORTS, apiCallId.get())
            .errorReports(errorReportResult).build();
    }

    public byte[] deleteErrorReports(
        @Nullable final Date since,
        @Nullable final Date to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        ApiCallRc rc = errorReporter.deleteErrorReports(since, to, exception, version, ids);

        return interComSerializer
            .answerBuilder(ApiConsts.API_DEL_ERROR_REPORT, apiCallId.get())
            .apiCallAnswerMsg(rc)
            .build();
    }

    public boolean modifyStltConfig(StltConfigOuterClass.StltConfig stltConfRef)
    {
        String logLevel = stltConfRef.getLogLevel();
        String logLevelLinstor = stltConfRef.getLogLevelLinstor();
        stltCfg.setLogLevel(logLevel);
        stltCfg.setLogLevelLinstor(logLevelLinstor);
        boolean successFlag = false;
        try
        {
            errorReporter.setLogLevel(
                apiCtx, Level.valueOf(logLevel.toUpperCase()), Level.valueOf(logLevelLinstor.toUpperCase())
            );
            successFlag = true;
        }
        catch (AccessDeniedException ignored)
        {
        }
        return successFlag;
    }


    private void deleteOldResFiles()
    {
        try
        {
            Path varDrbdPath = Paths.get(platformStlt.sysRoot() + LinStor.CONFIG_PATH);

            final Pattern keepResPattern = stltCfg.getDrbdKeepResPattern();
            Function<Path, Boolean> keepFunc;
            if (keepResPattern != null)
            {
                errorReporter.logInfo(
                    "Removing res files from " + varDrbdPath + ", keeping files matching regex: " +
                        keepResPattern.pattern()
                );
                keepFunc = (path) -> keepResPattern.matcher(path.getFileName().toString()).find();
            }
            else
            {
                errorReporter.logInfo("Removing all res files from " + varDrbdPath);
                keepFunc = (path) -> false;
            }
            try (Stream<Path> files = Files.list(varDrbdPath))
            {
                files.filter(path -> path.toString().endsWith(".res"))
                    .forEach(
                        filteredPathName ->
                        {
                            try
                            {
                                if (!keepFunc.apply(filteredPathName))
                                {
                                    Files.delete(filteredPathName);
                                }
                            }
                            catch (IOException ioExc)
                            {
                                throw new ImplementationError(
                                    "Unable to delete drbd resource file: " + filteredPathName,
                                    ioExc
                                );
                            }
                        }
                    );
            }
        }
        catch (IOException ioExc)
        {
            throw new ImplementationError(
                "Unable to list content of: " + platformStlt.sysRoot() + LinStor.CONFIG_PATH,
                ioExc
            );
        }
    }

    private interface ApplyData
    {
        long getFullSyncId();
        long getUpdateId();

        default boolean needReconfigurationWriteLock()
        {
            return false;
        }

        void applyChange();
    }

    private class ApplyControllerData implements ApplyData
    {
        private final Map<String, String> satelliteProps;
        private long fullSyncId;
        private long updateId;

        ApplyControllerData(Map<String, String> satellitePropsRef, long fullSyncIdRef, long updateIdRef)
        {
            satelliteProps = satellitePropsRef;
            fullSyncId = fullSyncIdRef;
            updateId = updateIdRef;
        }

        @Override
        public long getFullSyncId()
        {
            return fullSyncId;
        }

        @Override
        public long getUpdateId()
        {
            return updateId;
        }

        @Override
        public boolean needReconfigurationWriteLock()
        {
            return true;
        }

        @Override
        public void applyChange()
        {
            doApplyControllerChanges(satelliteProps, false);
        }
    }

    private class ApplyNode implements ApplyData
    {
        private @Nullable NodePojo nodePojo;
        private @Nullable String deletedNodeName;
        private long fullSyncId;
        private long updateId;

        ApplyNode(NodePojo nodePojoRef)
        {
            nodePojo = nodePojoRef;
            deletedNodeName = null;
            this.fullSyncId = nodePojoRef.getFullSyncId();
            this.updateId = nodePojoRef.getUpdateId();
        }

        ApplyNode(String nodeNameRef, long fullSyncIdRef, long updateIdRef)
        {
            nodePojo = null;
            deletedNodeName = nodeNameRef;
            fullSyncId = fullSyncIdRef;
            updateId = updateIdRef;
        }

        @Override
        public long getFullSyncId()
        {
            return fullSyncId;
        }

        @Override
        public long getUpdateId()
        {
            return updateId;
        }

        @Override
        public void applyChange()
        {
            try (LockGuard ls = LockGuard.createLocked(nodesMapLock.writeLock()))
            {
                if (nodePojo != null)
                {
                    nodeHandler.applyChanges(nodePojo);
                }
                else
                {
                    nodeHandler.applyDeletedNode(deletedNodeName);
                }
            }
        }
    }

    private class ApplyRscData implements ApplyData
    {
        private @Nullable RscPojo rscPojo;
        private @Nullable String deletedRscName;
        private long fullSyncId;
        private long updateId;

        ApplyRscData(RscPojo rscPojoRef)
        {
            rscPojo = rscPojoRef;
            fullSyncId = rscPojo.getFullSyncId();
            updateId = rscPojo.getUpdateId();
        }

        ApplyRscData(
            String rscNameRef,
            long fullSyncIdRef,
            long updateIdRef
        )
        {
            deletedRscName = rscNameRef;
            this.fullSyncId = fullSyncIdRef;
            this.updateId = updateIdRef;
        }

        @Override
        public long getFullSyncId()
        {
            return fullSyncId;
        }

        @Override
        public long getUpdateId()
        {
            return updateId;
        }

        @Override
        public void applyChange()
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    nodesMapLock.writeLock(),
                    rscDfnMapLock.writeLock()
                )
            )
            {
                if (rscPojo != null)
                {
                    rscHandler.applyChanges(rscPojo);
                    checkForAlreadyKnownResources(rscPojo, false);
                }
                else
                {
                    rscHandler.applyDeletedRsc(deletedRscName);
                }
            }
        }
    }

    private class ApplyStorPool implements ApplyData
    {
        private @Nullable StorPoolPojo storPoolPojo;
        private @Nullable String deletedStorPoolName;
        private long fullSyncId;
        private long updateId;

        ApplyStorPool(StorPoolPojo storPoolPojoRef)
        {
            storPoolPojo = storPoolPojoRef;
            fullSyncId = storPoolPojo.getFullSyncId();
            updateId = storPoolPojo.getUpdateId();
        }

        ApplyStorPool(String storPoolNameRef, long fullSyncIdRef, long updateIdRef)
        {
            deletedStorPoolName = storPoolNameRef;
            fullSyncId = fullSyncIdRef;
            updateId = updateIdRef;
        }

        @Override
        public long getFullSyncId()
        {
            return fullSyncId;
        }

        @Override
        public long getUpdateId()
        {
            return updateId;
        }

        @Override
        public void applyChange()
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    nodesMapLock.writeLock(),
                    storPoolDfnMapLock.writeLock()
                )
            )
            {
                if (storPoolPojo != null)
                {
                    storPoolHandler.applyChanges(storPoolPojo);
                }
                else
                {
                    storPoolHandler.applyDeletedStorPool(deletedStorPoolName);
                }
            }
        }
    }

    private class ApplySnapshot implements ApplyData
    {
        private final SnapshotPojo snapshotPojo;

        ApplySnapshot(SnapshotPojo snapshotPojoRef)
        {
            snapshotPojo = snapshotPojoRef;
        }

        @Override
        public long getFullSyncId()
        {
            return snapshotPojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return snapshotPojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    rscDfnMapLock.writeLock()
                )
            )
            {
                snapshotHandler.applyChanges(snapshotPojo);
            }
        }
    }

    private class ApplyEndedSnapshot implements ApplyData
    {
        private final String rscName;
        private final String snapshotName;
        private final long fullSyncId;
        private final long updateId;

        ApplyEndedSnapshot(
            String rscNameRef,
            String snapshotNameRef,
            long fullSyncIdRef,
            long updateIdRef
        )
        {
            rscName = rscNameRef;
            snapshotName = snapshotNameRef;
            fullSyncId = fullSyncIdRef;
            updateId = updateIdRef;
        }

        @Override
        public long getFullSyncId()
        {
            return fullSyncId;
        }

        @Override
        public long getUpdateId()
        {
            return updateId;
        }

        @Override
        public void applyChange()
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    rscDfnMapLock.writeLock()
                )
            )
            {
                snapshotHandler.applyEndedSnapshot(rscName, snapshotName);
            }
        }
    }

    private class ApplyCryptKey implements ApplyData
    {
        private final byte[] cryptKey;
        private final byte[] hash;
        private final byte[] salt;
        private final byte[] encKey;
        private final long fullSyncId;
        private final long updateId;

        ApplyCryptKey(
            byte[] cryptKeyRef,
            byte[] hashRef,
            byte[] saltRef,
            byte[] encKeyRef,
            long fullSyncIdRef,
            long updateIdRef
        )
        {
            cryptKey = cryptKeyRef;
            hash = hashRef;
            salt = saltRef;
            encKey = encKeyRef;
            fullSyncId = fullSyncIdRef;
            updateId = updateIdRef;
        }

        @Override
        public long getFullSyncId()
        {
            return fullSyncId;
        }

        @Override
        public long getUpdateId()
        {
            return updateId;
        }

        @Override
        public void applyChange()
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    nodesMapLock.writeLock(),
                    rscDfnMapLock.writeLock(),
                    storPoolDfnMapLock.writeLock()
                )
            )
            {
                stltSecObj.setCryptKey(cryptKey, hash, salt, encKey);

                vlmDfnHandler.decryptVolumesAndDrives(true);
            }
        }
    }

    private class ApplyExternalFile implements ApplyData
    {
        private final ExternalFilePojo externalFilePojo;
        private final boolean deleted;

        ApplyExternalFile(ExternalFilePojo externalFilePojoRef, boolean deletedRef)
        {
            externalFilePojo = externalFilePojoRef;
            deleted = deletedRef;
        }

        @Override
        public long getFullSyncId()
        {
            return externalFilePojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return externalFilePojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    extFileMapLock.writeLock()
                )
            )
            {
                if (deleted)
                {
                    extFilesHandler.applyDeletedExtFile(externalFilePojo);
                }
                else
                {
                    extFilesHandler.applyChanges(externalFilePojo);
                }
            }
        }
    }

    private class ApplyS3Remote implements ApplyData
    {
        private final S3RemotePojo s3remotePojo;

        ApplyS3Remote(S3RemotePojo s3remotePojoRef)
        {
            s3remotePojo = s3remotePojoRef;
        }

        @Override
        public long getFullSyncId()
        {
            return s3remotePojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return s3remotePojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    remoteMapLock.writeLock()
                )
            )
            {
                remoteHandler.applyChangesS3(s3remotePojo);
            }
        }
    }

    private class ApplyStltRemote implements ApplyData
    {
        private final StltRemotePojo stltRemotePojo;
        private final boolean deleted;

        ApplyStltRemote(StltRemotePojo stltRemotePojoRef, boolean deletedRef)
        {
            stltRemotePojo = stltRemotePojoRef;
            deleted = deletedRef;
        }

        @Override
        public long getFullSyncId()
        {
            return stltRemotePojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return stltRemotePojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try (LockGuard ls = LockGuard.createLocked(remoteMapLock.writeLock()))
            {
                if (deleted)
                {
                    remoteHandler.applyDeletedStltRemote(stltRemotePojo);
                }
                else
                {
                    remoteHandler.applyChangesStlt(stltRemotePojo);
                }
            }
        }
    }

    private class ApplyEbsRemote implements ApplyData
    {
        private final EbsRemotePojo ebsRemotePojo;

        ApplyEbsRemote(EbsRemotePojo ebsRemotePojoRef)
        {
            ebsRemotePojo = ebsRemotePojoRef;
        }

        @Override
        public long getFullSyncId()
        {
            return ebsRemotePojo.getFullSyncId();
        }

        @Override
        public long getUpdateId()
        {
            return ebsRemotePojo.getUpdateId();
        }

        @Override
        public void applyChange()
        {
            try (LockGuard ls = LockGuard.createLocked(remoteMapLock.writeLock()))
            {
                remoteHandler.applyChangesEbs(ebsRemotePojo);
            }
        }
    }

    private class ApplyRemoteDelete implements ApplyData
    {
        private final String remoteNameStr;

        private final long fullSyncId;
        private final long updateId;

        ApplyRemoteDelete(String remoteNameStrRef, long fullSyncIdRef, long updateIdRef)
        {
            remoteNameStr = remoteNameStrRef;
            fullSyncId = fullSyncIdRef;
            updateId = updateIdRef;
        }

        @Override
        public long getFullSyncId()
        {
            return fullSyncId;
        }

        @Override
        public long getUpdateId()
        {
            return updateId;
        }

        @Override
        public void applyChange()
        {
            try (LockGuard ls = LockGuard.createLocked(remoteMapLock.writeLock()))
            {
                remoteHandler.applyDeletedRemote(remoteNameStr);
            }
        }
    }

    public static class MissingRequiredExtToolsStorageException extends StorageException
    {
        private static final long serialVersionUID = 5817885610287768097L;

        public MissingRequiredExtToolsStorageException(String messageRef)
        {
            super(messageRef);
        }
    }

}
