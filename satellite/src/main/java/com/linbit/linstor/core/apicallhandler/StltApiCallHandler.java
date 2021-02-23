package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ChildProcessHandler;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.api.prop.WhitelistPropsReconfigurator;
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
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.Watch;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventPublisher;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdResource;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdStateTracker;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdVolume;
import com.linbit.linstor.layer.drbd.utils.ConfFileBuilder;
import com.linbit.linstor.layer.storage.DeviceProvider;
import com.linbit.linstor.layer.storage.DeviceProviderMapper;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LinstorFile;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.common.StltConfigOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.locks.LockGuard;
import com.linbit.utils.FileCollector;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
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
    private final StltExtToolsChecker deviceLayerChecker;

    private final CtrlStltSerializer interComSerializer;

    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
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

    private final ExtCmdFactory extCmdFactory;

    private WhitelistPropsReconfigurator whiteListPropsReconfigurator;
    private WhitelistProps whitelistProps;

    private final Provider<Long> apiCallId;
    private DrbdStateTracker drbdStateTracker;
    private DrbdEventPublisher drbdEventPublisher;
    private DrbdVersion drbdVersion;

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
        StltExtToolsChecker deviceLayerCheckerRef,
        CtrlStltSerializer interComSerializerRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(LinStor.SATELLITE_PROPS) Props satellitePropsRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StltSecurityObjects stltSecObjRef,
        StltCryptApiCallHelper vlmDfnHandlerRef,
        EventBroker eventBrokerRef,
        WhitelistProps whiteListPropsRef,
        WhitelistPropsReconfigurator whiteListPropsReconfiguratorRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        DrbdStateTracker drbdStateTrackerRef,
        DrbdEventPublisher drbdEventPublisherRef,
        DeviceProviderMapper deviceProviderMapperRef,
        DrbdVersion drbdVersionRef,
        ExtCmdFactory extCmdFactoryRef
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
        deviceLayerChecker = deviceLayerCheckerRef;
        interComSerializer = interComSerializerRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        transMgrProvider = transMgrProviderRef;
        stltSecObj = stltSecObjRef;
        vlmDfnHandler = vlmDfnHandlerRef;
        stltConf = satellitePropsRef;
        eventBroker = eventBrokerRef;
        whitelistProps = whiteListPropsRef;
        whiteListPropsReconfigurator = whiteListPropsReconfiguratorRef;
        apiCallId = apiCallIdRef;
        drbdStateTracker = drbdStateTrackerRef;
        drbdEventPublisher = drbdEventPublisherRef;
        deviceProviderMapper = deviceProviderMapperRef;
        drbdVersion = drbdVersionRef;
        extCmdFactory = extCmdFactoryRef;

        dataToApply = new TreeMap<>();
    }

    public AuthenticationResult authenticate(
        UUID nodeUuid,
        String nodeName,
        Peer controllerPeer
    )
    {
        AuthenticationResult authResult;

        synchronized (dataToApply)
        {
            dataToApply.clear(); // controller should not have sent us anything before the authentication.
            // that means, everything in this map is out-dated data + we should receive a full sync next.
        }

        controllerPeerConnector.setControllerPeer(
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
                deviceLayerChecker.getExternalTools(),
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

    public boolean applyFullSync(
        Map<String, String> satelliteProps,
        Set<NodePojo> nodes,
        Set<StorPoolPojo> storPools,
        Set<RscPojo> resources,
        Set<SnapshotPojo> snapshots,
        long fullSyncId,
        byte[] cryptKey
    )
    {
        boolean success = false;
        try (
            LockGuard ls = LockGuard.createLocked(
                reconfigurationLock.writeLock(),
                nodesMapLock.writeLock(),
                rscDfnMapLock.writeLock(),
                storPoolDfnMapLock.writeLock()
            )
        )
        {
            if (updateMonitor.getCurrentFullSyncId() == fullSyncId)
            {
                // only apply this fullSync if it is newer than the last one

                // clear all data
                nodesMap.clear();
                rscDfnMap.clear();
                storPoolDfnMap.clear();

                for (NodePojo node : nodes)
                {
                    Node curNode = nodeHandler.applyChanges(node);
                    if (curNode != null)
                    {
                        nodesMap.put(curNode.getName(), curNode);
                    }
                }
                controllerPeerConnector.setControllerPeerToCurrentLocalNode();

                doApplyControllerChanges(satelliteProps);

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
                    rscHandler.applyChanges(rsc);
                }

                for (SnapshotPojo snapshot : snapshots)
                {
                    snapshotHandler.applyChanges(snapshot);
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
                    errorReporter.logTrace("Resource '" + rsc.getName() + "' created.");
                }
                for (SnapshotPojo snapshot : snapshots)
                {
                    errorReporter.logTrace("Snapshot '" + snapshot.getSnaphotDfn() + "' created.");
                }
                errorReporter.logTrace("Full sync with controller finished");

                // Atomically notify the DeviceManager to check all resources
                Node localNode = controllerPeerConnector.getLocalNode();
                if (localNode != null)
                {
                    if (deviceManager != null)
                    {
                        deviceManager.fullSyncApplied(localNode);
                    }
                }
                else
                {
                    errorReporter.logWarning(
                        "No node object that represents this satellite was received from the controller"
                    );
                }

                if (cryptKey != null && cryptKey.length > 0)
                {
                    stltSecObj.setCryptKey(cryptKey);

                    vlmDfnHandler.decryptAllNewLuksVlmKeys(true);
                }

                whiteListPropsReconfigurator.reconfigure();

                updateMonitor.setFullSyncApplied();

                errorReporter.logTrace("FullSync registered");

                // There are no explicit controller - satellite watches.
                // FullSync implicitly creates a watch for all events.
                createWatchForPeer();

                for (RscPojo rsc : resources)
                {
                    checkForAlreadyKnownResources(rsc);
                }

            }
            else
            {
                errorReporter.logWarning(
                    "Ignored an incoming but outdated fullsync (%d, expected: %d)",
                    fullSyncId,
                    updateMonitor.getCurrentFullSyncId()
                );
            }
            success = true;
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);

            success = false;
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
        return success;
    }

    private void checkForAlreadyKnownResources(RscPojo rsc)
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

        DrbdResource drbdResource = drbdStateTracker.getResource(rsc.getName());
        if (drbdResource != null && !drbdResource.isKnownByLinstor())
        {
            drbdResource.setKnownByLinstor(true);
            drbdEventPublisher.resourceCreated(drbdResource);

            Iterator<DrbdVolume> itVlm = drbdResource.iterateVolumes();
            while (itVlm.hasNext())
            {
                DrbdVolume drbdVlm = itVlm.next();
                drbdEventPublisher.volumeCreated(drbdResource, null, drbdVlm);

            }
        }
    }

    private void createWatchForPeer()
    {
        Peer controllerPeer = controllerPeerConnector.getControllerPeer();
        eventBroker.createWatch(controllerPeer, new Watch(
            UUID.randomUUID(), controllerPeer.getId(), 0, EventIdentifier.global(null)
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

    private void doApplyControllerChanges(Map<String, String> satelliteProps)
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

            transMgrProvider.get().commit();

            regenerateLinstorCommonConf();

            reconfigureAllStorageDrivers();

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

    private void regenerateLinstorCommonConf()
    {
        if (drbdVersion.hasDrbd9())
        {
            Path tmpResFileOut = Paths.get(CoreModule.CONFIG_PATH + "/" + "linstor-common.tmp");
            Node.Type localNodeType;
            try
            {
                localNodeType = controllerPeerConnector.getLocalNode().getNodeType(apiCtx);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            if (tmpResFileOut != null && !localNodeType.equals(Node.Type.OPENFLEX_TARGET))
            {
                try (
                    FileOutputStream commonFileOut = new FileOutputStream(tmpResFileOut.toFile())
                )
                {
                    ConfFileBuilder confFileBuilder = new ConfFileBuilder(
                        errorReporter,
                        whitelistProps,
                        stltConf
                    );
                    commonFileOut.write(confFileBuilder.buildCommonConf(stltConf).getBytes());
                }
                catch (IOException ioExc)
                {
                    String ioErrorMsg = ioExc.getMessage();
                    if (ioErrorMsg == null)
                    {
                        ioErrorMsg = "The runtime environment or operating system did not provide a " +
                            "description of the I/O error";
                    }

                    errorReporter.reportError(
                        Level.ERROR,
                        new LinStorException(
                            "Creation of the common Linstor DRBD configuration file " +
                                "'linstor_common.conf' failed due to an I/O error",
                            null,
                            "Creation of the DRBD configuration file failed due to an I/O error",
                            "- Check whether enough free space is available for the creation of the file\n" +
                                "- Check whether the application has write access to the target directory\n" +
                                "- Check whether the storage is operating flawlessly",
                            "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                            ioExc
                        ),
                        apiCtx,
                        null,
                        ioErrorMsg
                    );
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    throw new ImplementationError(accDeniedExc);
                }

                try
                {
                    Files.move(
                        tmpResFileOut,
                        Paths.get(CoreModule.CONFIG_PATH + "/linstor_common.conf"),
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE
                    );
                }
                catch (IOException ioExc)
                {
                    String ioErrorMsg = ioExc.getMessage();
                    errorReporter.reportError(
                        Level.ERROR,
                        new LinStorException(
                            "Unable to move temporary common Linstor DRBD configuration file " +
                                "'linstor_common.conf' failed due to an I/O error",
                            null,
                            "Creation of the DRBD configuration file failed due to an I/O error",
                            "- Check whether enough free space is available for the creation of the file\n" +
                                "- Check whether the application has write access to the target directory\n" +
                                "- Check whether the storage is operating flawlessly",
                            "The error reported by the runtime environment or operating system is:\n" + ioErrorMsg,
                            ioExc
                        ),
                        apiCtx,
                        null,
                        ioErrorMsg
                    );
                }
            }
        }
    }

    private void reconfigureAllStorageDrivers()
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
                        StorPool sp = spd.getStorPool(apiCtx, controllerPeerConnector.getLocalNodeName());
                        if (sp != null) { // storage pool probably not deployed on this node
                            DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderByStorPool(sp);
                            deviceProvider.checkConfig(sp);
                        }
                    }
                    catch (StorageException exc)
                    {
                        errorReporter.reportError(exc);
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

    public void setCryptKey(byte[] key, long fullSyncId, long updateId)
    {
        applyChangedData(new ApplyCryptKey(key, fullSyncId, updateId));
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

    public byte[] listErrorReports(
        final Set<String> nodes,
        boolean withContent,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids
    )
    {
        List<ErrorReport> errorReports = errorReporter.listReports(
            withContent,
            since,
            to,
            ids
        );

        return interComSerializer.answerBuilder(ApiConsts.API_LST_ERROR_REPORTS, apiCallId.get())
            .errorReports(errorReports).build();
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

    public byte[] listSosReport(Date since)
    {
        String nodeName = controllerPeerConnector.getLocalNode().getName().getDisplayName();
        Set<LinstorFile> reports = new HashSet<>();
        reports.add(
            new LinstorFile(
                nodeName,
                "linstorInfo",
                new Date(System.currentTimeMillis()),
                LinStor.linstorInfo() + "\n\nuname -a:           " + LinStor.getUname("-a")
            )
        );

        try (Stream<Path> files = Files.list(Paths.get("/var/lib/linstor.d/")))
        {
            CommandHelper[] commands = new CommandHelper[]
            {
                new CommandHelper(
                    "drbd-status",
                    new String[]
                    {"drbdsetup", "status", "-vvv"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "drbd-events2",
                    new String[]
                    {"drbdsetup", "events2", "all", "--now"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "modinfo",
                    new String[]
                    {"modinfo", "drbd"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "proc-drbd",
                    new String[]
                    {"cat", "/proc/drbd"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "lvm.conf",
                    new String[]
                    {"cat", "/etc/lvm/lvm.conf"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    StltConfig.LINSTOR_STLT_CONFIG,
                    new String[]
                    {"cat", stltCfg.getConfigDir() + StltConfig.LINSTOR_STLT_CONFIG},
                    this::makeFileFromCmdNoErrRep
                ),
                new CommandHelper(
                    "dmesg",
                    new String[]
                    {"dmesg", "-H"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "journalctl",
                    new String[]
                    {"journalctl", "-u", "linstor-satellite", "--since", LinStor.JOURNALCTL_DF.format(since)},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "ip-a",
                    new String[]
                    {"ip", "a"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "drbdadm-version",
                    new String[]
                    {"drbdadm", "--version"},
                    this::makeFileFromCmd
                ),
                new CommandHelper(
                    "log-syslog",
                    new String[]
                    {"cat", "/var/log/syslog"},
                    this::makeFileFromCmdNoErrRep
                ),
                new CommandHelper(
                    "log-kern.log",
                    new String[]
                    {"cat", "/var/log/kern.log"},
                    this::makeFileFromCmdNoErrRep
                ),
                new CommandHelper(
                    "log-messages",
                    new String[]
                    {"cat", "/var/log/messages"},
                    this::makeFileFromCmdNoErrRep
                ),
                new CommandHelper(
                    "release",
                    new String[]
                    {"cat", "/etc/redhat-release", "/etc/lsb-release", "/etc/os-release"},
                    this::makeFileFromCmdNoFailed
                )
            };
            for (CommandHelper cmd : commands)
            {
                reports.add(cmd.handleExitCode.accept(nodeName, cmd.cmd, cmd.file));
            }

            files.filter(file -> file.getFileName().toString().endsWith(".res"))
                .forEach(file ->
                {
                    try
                    {
                        BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
                        String content = "";
                        try
                        {
                            content = new String(Files.readAllBytes(file));
                        }
                        catch (IOException ignored)
                        {
                        }
                        reports.add(
                            new LinstorFile(
                                nodeName,
                                "res/" + file.getFileName().toString(),
                                new Date(attr.creationTime().toMillis()),
                                content
                            )
                        );
                    }
                    catch (IOException exc)
                    {
                        String errReportName = errorReporter.reportError(exc);
                        reports.add(
                            new LinstorFile(
                                nodeName,
                                file.getFileName().toString() + ".failed",
                                new Date(System.currentTimeMillis()),
                                errReportName
                            )
                        );
                    }
                });
            FileCollector collector = new FileCollector(nodeName, errorReporter.getLogDirectory());
            Files.walkFileTree(errorReporter.getLogDirectory(), collector);
            reports.addAll(collector.getFiles());
        }
        catch (IOException ignored)
        {
        }
        return interComSerializer.answerBuilder(InternalApiConsts.API_RSP_SOS_REPORT, apiCallId.get())
            .sosReport(nodeName, reports)
            .build();
    }

    private LinstorFile makeFileFromCmd(String nodeName, String[] command, String fileName) {
        try
        {
            OutputData output = extCmdFactory.create().exec(command);
            if (output.exitCode != 0)
            {
                String errReportName = errorReporter.reportError(new ExtCmdFailedException(command, output));
                return new LinstorFile(
                    nodeName,
                    fileName + ".failed",
                    new Date(System.currentTimeMillis()),
                    "ErrorReport-" + errReportName
                );
            }
            else
            {
                return new LinstorFile(
                    nodeName,
                    fileName,
                    new Date(System.currentTimeMillis()),
                    new String(output.stdoutData)
                );
            }
        }
        catch (Exception exc)
        {
            String errReportName = errorReporter.reportError(exc);
            return new LinstorFile(
                nodeName,
                fileName + ".failed",
                new Date(System.currentTimeMillis()),
                errReportName
            );
        }
    }

    private LinstorFile makeFileFromCmdNoFailed(String nodeName, String[] command, String fileName)
    {
        try
        {
            OutputData output = extCmdFactory.create().exec(command);
            if (output.exitCode != 0)
            {
                return new LinstorFile(
                    nodeName,
                    fileName,
                    new Date(System.currentTimeMillis()),
                    new String(output.stdoutData) + "\n\n" + new String(output.stderrData)
                );
            }
            else
            {
                return new LinstorFile(
                    nodeName,
                    fileName,
                    new Date(System.currentTimeMillis()),
                    new String(output.stdoutData)
                );
            }
        }
        catch (Exception exc)
        {
            String errReportName = errorReporter.reportError(exc);
            return new LinstorFile(
                nodeName,
                fileName + ".failed",
                new Date(System.currentTimeMillis()),
                errReportName
            );
        }
    }

    private LinstorFile makeFileFromCmdNoErrRep(String nodeName, String[] command, String fileName)
    {
        try
        {
            OutputData output = extCmdFactory.create().exec(command);
            if (output.exitCode != 0)
            {
                return new LinstorFile(
                    nodeName,
                    fileName + ".failed",
                    new Date(System.currentTimeMillis()),
                    new String(output.stderrData)
                );
            }
            else
            {
                return new LinstorFile(
                    nodeName,
                    fileName,
                    new Date(System.currentTimeMillis()),
                    new String(output.stdoutData)
                );
            }
        }
        catch (Exception exc)
        {
            String errReportName = errorReporter.reportError(exc);
            return new LinstorFile(
                nodeName,
                fileName + ".failed",
                new Date(System.currentTimeMillis()),
                errReportName
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

    private interface TriFunction<T, U, V, W>
    {
        W accept(T arg1, U arg2, V arg3);
    }

    private class CommandHelper
    {
        String file;
        String[] cmd;
        TriFunction<String, String[], String, LinstorFile> handleExitCode;

        CommandHelper(
            String fileRef,
            String[] cmdRef,
            TriFunction<String, String[], String, LinstorFile> handleExitCodeRef
        )
        {
            file = fileRef;
            cmd = cmdRef;
            handleExitCode = handleExitCodeRef;
        }
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
            doApplyControllerChanges(satelliteProps);
        }
    }

    private class ApplyNode implements ApplyData
    {
        private NodePojo nodePojo;
        private String deletedNodeName;
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
        private RscPojo rscPojo;
        private String deletedRscName;
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
                    checkForAlreadyKnownResources(rscPojo);
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
        private StorPoolPojo storPoolPojo;
        private String deletedStorPoolName;
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
        private final long fullSyncId;
        private final long updateId;

        ApplyCryptKey(byte[] cryptKeyRef, long fullSyncIdRef, long updateIdRef)
        {
            cryptKey = cryptKeyRef;
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
                stltSecObj.setCryptKey(cryptKey);

                vlmDfnHandler.decryptAllNewLuksVlmKeys(true);
            }
        }
    }
}
