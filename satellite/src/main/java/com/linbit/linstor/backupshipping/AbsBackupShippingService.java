package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.backups.BackupMetaInfoPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltConnTracker;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition.Flags;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.TimeUtils;

import javax.inject.Inject;

import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

public abstract class AbsBackupShippingService implements SystemService
{
    public final ServiceName serviceName;

    private final RemoteType remoteType;
    protected final Map<PairNonNull<Snapshot, RemoteName>, ShippingInfo> shippingInfoMap;
    private final Set<PairNonNull<Snapshot, RemoteName>> startedShippments;
    private final Map<PairNonNull<Snapshot, RemoteName>, List<String>> finishedShipments;
    protected final ThreadGroup threadGroup;
    protected final AccessContext accCtx;
    protected final RemoteMap remoteMap;
    protected final LockGuardFactory lockGuardFactory;

    private ServiceName instanceName;
    private boolean serviceStarted = false;
    protected ErrorReporter errorReporter;
    protected ExtCmdFactory extCmdFactory;
    protected ControllerPeerConnector controllerPeerConnector;
    protected CtrlStltSerializer interComSerializer;
    protected StltSecurityObjects stltSecObj;
    protected StltConfigAccessor stltConfigAccessor;

    @Inject
    public AbsBackupShippingService(
        ErrorReporter errorReporterRef,
        String serviceNameRef,
        RemoteType remoteTypeRef,
        ExtCmdFactory extCmdFactoryRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer interComSerializerRef,
        @SystemContext AccessContext accCtxRef,
        StltSecurityObjects stltSecObjRef,
        StltConfigAccessor stltConfigAccessorRef,
        StltConnTracker stltConnTracker,
        RemoteMap remoteMapRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        remoteType = remoteTypeRef;
        extCmdFactory = extCmdFactoryRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        accCtx = accCtxRef;
        stltSecObj = stltSecObjRef;
        stltConfigAccessor = stltConfigAccessorRef;
        remoteMap = remoteMapRef;
        lockGuardFactory = lockGuardFactoryRef;

        try
        {
            serviceName = new ServiceName(serviceNameRef);
            instanceName = new ServiceName(serviceNameRef);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        shippingInfoMap = Collections.synchronizedMap(new TreeMap<>());
        startedShippments = Collections.synchronizedSet(new TreeSet<>());
        finishedShipments = Collections.synchronizedMap(new TreeMap<>());
        threadGroup = new ThreadGroup("SnapshotShippingSerivceThreadGroup");

        // this causes all shippings to be aborted should the satellite lose connection to the controller
        stltConnTracker.addClosingListener((peer, isCtrl) ->
        {
            if (isCtrl)
            {
                killAllShipping(false);
            }
        });
    }

    public void killAllShipping(boolean doPostShipping)
    {
        Map<PairNonNull<Snapshot, RemoteName>, ShippingInfo> copy = new TreeMap<>(shippingInfoMap);
        for (ShippingInfo shippingInfo : copy.values())
        {
            for (SnapVlmDataInfo snapVlmDataInfo : shippingInfo.snapVlmDataInfoMap.values())
            {
                snapVlmDataInfo.daemon.shutdown(doPostShipping);
                try
                {
                    snapVlmDataInfo.daemon.awaitShutdown(750);
                }
                catch (InterruptedException exc)
                {
                    errorReporter.reportError(exc);
                }
            }
        }
        shippingInfoMap.clear();
        synchronized (startedShippments)
        {
            startedShippments.clear();
        }
    }

    public void abort(AbsStorageVlmData<Snapshot> snapVlmData, String remoteName, boolean sendPostShipping)
    {
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        ShippingInfo info = shippingInfoMap.get(getShipKey(remoteName, snap));
        shutdownDaemonsDuringAbort(snapVlmData, sendPostShipping, info);
    }

    private void shutdownDaemonsDuringAbort(
        AbsStorageVlmData<Snapshot> snapVlmData,
        boolean sendPostShipping,
        ShippingInfo info
    )
    {
        errorReporter.logDebug(
            "[%s] aborting backup shipping: %s, RscSuffix: '%s', VlmNr: '%s'",
            remoteType.name(),
            snapVlmData.getRscLayerObject().getAbsResource().toString(),
            snapVlmData.getRscLayerObject().getResourceNameSuffix(),
            snapVlmData.getVlmNr()
        );
        if (info != null)
        {
            SnapVlmDataInfo snapVlmDataInfo = info.snapVlmDataInfoMap.get(snapVlmData);
            if (snapVlmDataInfo != null)
            {
                snapVlmDataInfo.daemon.shutdown(sendPostShipping);
            }
        }
        else
        {
            errorReporter.logDebug(
                "[%s]  backupShippingInfo is null, nothing to shutdown",
                remoteType.name()
            );
        }
    }

    public void abortAll(AbsStorageVlmData<Snapshot> snapVlmData, boolean sendPostShipping)
    {
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        for (Entry<PairNonNull<Snapshot, RemoteName>, ShippingInfo> shipInfoEntry : shippingInfoMap.entrySet())
        {
            if (shipInfoEntry.getKey().objA.equals(snap))
            {
                shutdownDaemonsDuringAbort(snapVlmData, sendPostShipping, shipInfoEntry.getValue());
            }
        }
    }

    public void sendBackup(
        String snapNameRef,
        String rscNameRef,
        String rscNameSuffixRef,
        int vlmNrRef,
        String cmdRef,
        AbsStorageVlmData<Snapshot> basedOnSnapVlmData,
        AbsStorageVlmData<Snapshot> snapVlmData,
        String s3orLinRemoteName
    ) throws StorageException, InvalidNameException, InvalidKeyException, AccessDeniedException
    {
        if (RscLayerSuffixes.shouldSuffixBeShipped(rscNameSuffixRef))
        {
            String namespc = BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName;
            String s3orStltRemoteName = ((SnapshotVolume) snapVlmData.getVolume()).getSnapshot()
                .getSnapshotDefinition()
                .getSnapDfnProps(accCtx)
                .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, namespc);
            String s3Suffix = stltConfigAccessor.getReadonlyProps()
                .getProp(ApiConsts.KEY_BACKUP_S3_SUFFIX, ApiConsts.NAMESPC_BACKUP_SHIPPING);
            String backupTimeRaw = ((SnapshotVolume) snapVlmData.getVolume()).getSnapshotDefinition()
                .getSnapDfnProps(accCtx)
                .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, namespc);
            LocalDateTime backupTime = TimeUtils.millisToDate(Long.parseLong(backupTimeRaw));

            String backupName = new S3VolumeNameInfo(
                rscNameRef,
                rscNameSuffixRef,
                vlmNrRef,
                backupTime,
                s3Suffix,
                snapNameRef
            ).toString();

            AbsRemote s3orStltRemote = remoteMap.get(new RemoteName(s3orStltRemoteName, true));
            startDaemon(
                cmdRef,
                new String[]
                {
                    "timeout",
                    "0",
                    "bash",
                    "-c",
                    getCommandSending(
                        cmdRef,
                        s3orStltRemote,
                        snapVlmData
                    )
                },
                backupName,
                s3orStltRemote,
                false,
                null,
                (success, addrInUse) -> postShipping(
                    success,
                    addrInUse,
                    snapVlmData,
                    InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_SENT,
                    true,
                    false,
                    s3orStltRemote instanceof StltRemote ? ((StltRemote) s3orStltRemote).getLinstorRemoteName().displayValue :
                        s3orStltRemoteName
                ),
                basedOnSnapVlmData,
                snapVlmData
            );
        }
    }

    public void restoreBackup(
        String cmdRef,
        AbsStorageVlmData<Snapshot> snapVlmData
    ) throws StorageException, AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        if (RscLayerSuffixes.shouldSuffixBeShipped(snapVlmData.getRscLayerObject().getResourceNameSuffix()))
        {
            SnapshotVolume snapVlm = (SnapshotVolume) snapVlmData.getVolume();
            String remoteName = snapVlm.getSnapshot()
                .getSnapshotDefinition()
                .getSnapDfnProps(accCtx)
                .getProp(
                    InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                    BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC
                );
            AbsRemote remote = remoteMap.get(new RemoteName(remoteName, true));

            ensureRemoteType(remote);
            Integer port = null;
            if (remote instanceof StltRemote)
            {
                port = ((StltRemote) remote).getPorts(accCtx)
                    .get(snapVlmData.getVlmNr() + snapVlmData.getRscLayerObject().getResourceNameSuffix());
            }

            String backupName = getBackupNameForRestore(snapVlmData);
            startDaemon(
                cmdRef,
                new String[]
                {
                    "timeout",
                    "0",
                    "bash",
                    "-c",
                    getCommandReceiving(cmdRef, remote, snapVlmData)
                },
                backupName,
                remote,
                true,
                port,
                (success, addrInUse) -> postShipping(
                    success,
                    addrInUse,
                    snapVlmData,
                    InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_RECEIVED,
                    true,
                    true,
                    remote instanceof StltRemote ? ((StltRemote) remote).getLinstorRemoteName().displayValue :
                        remoteName
                ),
                null,
                snapVlmData
            );
        }
    }

    public void allBackupPartsRegistered(Snapshot snap, String s3orStltRemoteName)
    {
        ShippingInfo info = null;
        boolean justStarted = false;
        // assign to new object so the code analyzer doesn't complain about using a method parameter for syncing
        final Object syncObj = snap;
        synchronized (syncObj)
        {
            AbsRemote remote;
            try
            {
                remote = remoteMap.get(new RemoteName(s3orStltRemoteName, true));
            }
            catch (InvalidNameException exc)
            {
                throw new ImplementationError(exc);
            }
            RemoteName s3orLinRemoteName = remote instanceof StltRemote ? ((StltRemote) remote).getLinstorRemoteName() :
                remote.getName();
            PairNonNull<Snapshot, RemoteName> shipKey = getShipKey(s3orLinRemoteName.displayValue, snap);
            info = shippingInfoMap.get(shipKey);
            if (info != null)
            {
                synchronized (info)
                {
                    if (!info.isStarted)
                    {
                        for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
                        {
                            String uploadId = snapVlmDataInfo.daemon.start();
                            synchronized (startedShippments)
                            {
                                startedShippments.add(shipKey);
                            }

                            if (uploadId != null)
                            {
                                SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
                                controllerPeerConnector.getControllerPeer()
                                    .sendMessage(
                                        interComSerializer
                                            .onewayBuilder(InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_ID)
                                            .notifyBackupShippingId(
                                                snap,
                                                snapVlmDataInfo.backupName,
                                                uploadId,
                                                s3orStltRemoteName
                                            )
                                            .build(),
                                        InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_ID
                                    );
                            }
                        }
                        justStarted = true;
                        info.isStarted = true;
                    }
                    else
                    {
                        errorReporter.logTrace("Backup shipping for snap %s already in progress", snap);
                    }
                }
            }
        }
        if (info != null && justStarted)
        {
            postAllBackupPartsRegistered(snap, info);
        }
    }

    private void startDaemon(
        String sendRecvCommand,
        String[] fullCommand,
        String backupNameRef,
        AbsRemote s3orStltRemote,
        boolean restore,
        @Nullable Integer portRef,
        BiConsumer<Boolean, Integer> postAction,
        @Nullable AbsStorageVlmData<Snapshot> basedOnSnapVlmData,
        AbsStorageVlmData<Snapshot> snapVlmData
    )
        throws StorageException, AccessDeniedException
    {
        ensureRemoteType(s3orStltRemote);

        if (serviceStarted)
        {
            RemoteName s3orLinRemoteName = s3orStltRemote instanceof StltRemote ? ((StltRemote) s3orStltRemote)
                .getLinstorRemoteName() : s3orStltRemote.getName();
            if (!alreadyStarted(snapVlmData, s3orLinRemoteName.displayValue))
            {
                Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
                PairNonNull<Snapshot, RemoteName> shipKey = getShipKey(s3orLinRemoteName.displayValue, snap);
                ShippingInfo info = shippingInfoMap.computeIfAbsent(shipKey, key -> new ShippingInfo());
                if (s3orStltRemote instanceof StltRemote)
                {
                    info.portsUsed.add(
                        ((StltRemote) s3orStltRemote).getPorts(accCtx)
                            .get(snapVlmData.getVlmNr() + snapVlmData.getRscLayerObject().getResourceNameSuffix())
                    );
                }
                info.snapVlmDataInfoMap.put(
                    snapVlmData,
                    new SnapVlmDataInfo(
                        createDaemon(
                            snapVlmData,
                            fullCommand,
                            backupNameRef,
                            s3orStltRemote,
                            restore,
                            portRef,
                            postAction
                        ),
                        backupNameRef,
                        snapVlmData.getVlmNr().value
                    )
                );
                info.s3orStltRemote = s3orStltRemote;
                try
                {
                    String namespc;
                    if (restore)
                    {
                        namespc = BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC;
                    }
                    else
                    {
                        namespc = BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" +
                            (s3orStltRemote instanceof StltRemote ?
                                ((StltRemote) s3orStltRemote).getLinstorRemoteName().displayValue :
                                s3orStltRemote.getName().displayValue);
                    }
                    String s3Suffix = stltConfigAccessor.getReadonlyProps()
                        .getProp(ApiConsts.KEY_BACKUP_S3_SUFFIX, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                    String backupTimeRaw = ((SnapshotVolume) snapVlmData.getVolume()).getSnapshotDefinition()
                        .getSnapDfnProps(accCtx)
                        .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, namespc);
                    LocalDateTime backupTime = TimeUtils.millisToDate(Long.parseLong(backupTimeRaw));

                    info.s3MetaKey = new S3MetafileNameInfo(
                        snap.getResourceName().displayValue,
                        backupTime,
                        s3Suffix,
                        snap.getSnapshotName().displayValue
                    ).toString();

                    if (basedOnSnapVlmData != null && basedOnSnapVlmData != snapVlmData)
                    {
                        Snapshot basedOnSnap = basedOnSnapVlmData.getRscLayerObject().getAbsResource();
                        String basedOnSnapSuffix = basedOnSnap.getSnapshotDefinition()
                            .getSnapDfnProps(accCtx)
                            .getProp(ApiConsts.KEY_BACKUP_S3_SUFFIX, namespc);
                        String basedOnBackupTimeRaw = basedOnSnap.getSnapshotDefinition()
                            .getSnapDfnProps(accCtx)
                            .getProp(InternalApiConsts.KEY_BACKUP_START_TIMESTAMP, namespc);
                        LocalDateTime basedOnBackupTime = TimeUtils.millisToDate(Long.parseLong(basedOnBackupTimeRaw));

                        info.basedOnS3MetaKey = new S3MetafileNameInfo(
                            basedOnSnap.getResourceName().displayValue,
                            basedOnBackupTime,
                            basedOnSnapSuffix,
                            basedOnSnap.getSnapshotName().displayValue
                        ).toString();
                    }
                }
                catch (InvalidKeyException | AccessDeniedException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        }
        else
        {
            throw new StorageException("BackupShippingService not started");
        }
    }

    private PairNonNull<Snapshot, RemoteName> getShipKey(String remoteNameStr, Snapshot snap)
    {
        RemoteName remoteName;
        try
        {
            remoteName = new RemoteName(remoteNameStr, true);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return new PairNonNull<>(snap, remoteName);
    }

    private PairNonNull<Snapshot, RemoteName> getShipKey(AbsRemote remote, Snapshot snap)
    {
        RemoteName remoteForKey = remote instanceof StltRemote ?
            ((StltRemote) remote).getLinstorRemoteName() :
            remote.getName();
        return new PairNonNull<>(snap, remoteForKey);
    }

    /**
     * Throws an {@link ImplementationError} if <code>this.remoteType</code> does not equals to the parameter's
     * <code>remote.getType()</code>
     *
     * @param remote
     *
     * @throws ImplementationError
     */
    private void ensureRemoteType(AbsRemote remote) throws ImplementationError
    {
        if (!remoteType.equals(remote.getType()))
        {
            throw new ImplementationError(
                "Unexpected remote type. Parameter: " + remote.getType() + ", expected: " + remoteType
            );
        }
    }

    private void postShipping(
        boolean successRef,
        Integer portInUseRef,
        AbsStorageVlmData<Snapshot> snapVlmData,
        String internalApiName,
        boolean updateCtrlRef,
        boolean restoring,
        String s3orLinRemoteName
    )
    {
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        PairNonNull<Snapshot, RemoteName> shipKey = getShipKey(s3orLinRemoteName, snap);
        SnapshotDefinition.Key snapKey = null;
        boolean success = false;
        boolean done = false;
        Set<Integer> portsUsed = null;
        ShippingInfo shippingInfo = null;
        synchronized (snap)
        {
            shippingInfo = shippingInfoMap.get(shipKey);
            /*
             * shippingInfo might be already null as we delete it at the end of this method.
             */
            if (shippingInfo != null)
            {
                shippingInfo.snapVlmDataFinishedShipping++;
                if (portInUseRef != null)
                {
                    shippingInfo.alreadyInUse.add(portInUseRef);
                    shippingInfo.portsUsed.remove(portInUseRef);
                }
                if (successRef)
                {
                    shippingInfo.snapVlmDataFinishedSuccessfully++;
                    shippingInfo.snapVlmDataInfoMap.get(snapVlmData).finishTimestamp = System.currentTimeMillis();
                }
                if (shippingInfo.snapVlmDataFinishedShipping == shippingInfo.snapVlmDataInfoMap.size())
                {
                    if (updateCtrlRef)
                    {
                        if (!shippingInfo.alreadyInUse.isEmpty())
                        {
                            controllerPeerConnector.getControllerPeer().sendMessage(
                                interComSerializer.onewayBuilder(
                                    InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_WRONG_PORTS
                                ).notifyBackupShippingWrongPorts(
                                    shippingInfo.s3orStltRemote.getName().displayValue,
                                    snap.getSnapshotName().displayValue,
                                    snap.getResourceName().displayValue,
                                    shippingInfo.alreadyInUse
                                ).build(),
                                InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_WRONG_PORTS
                            );
                        }
                        else
                        {
                            success = shippingInfo.snapVlmDataFinishedSuccessfully == shippingInfo.snapVlmDataFinishedShipping;

                            snapKey = snap.getSnapshotDefinition().getSnapDfnKey();
                            portsUsed = shippingInfo.portsUsed;
                        }
                    }
                    done = true;

                    if (shippingInfo.alreadyInUse.isEmpty())
                    {
                        try
                        {
                            String namespc;
                            if (restoring)
                            {
                                namespc = BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC;
                            }
                            else
                            {
                                namespc = BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName;
                            }
                            String simpleBackupName = snap.getSnapshotDefinition()
                                .getSnapDfnProps(accCtx)
                                .getProp(InternalApiConsts.KEY_BACKUP_TO_RESTORE, namespc);
                            if (finishedShipments.containsKey(shipKey))
                            {
                                finishedShipments.get(shipKey).add(simpleBackupName);
                            }
                            else
                            {
                                finishedShipments.put(shipKey, new ArrayList<>(Arrays.asList(simpleBackupName)));
                            }
                        }
                        catch (InvalidKeyException | AccessDeniedException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                    }
                    shippingInfoMap.remove(shipKey);
                }
            }
        }

        if (snapKey != null)
        {
            try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.RSC_DFN_MAP, LockObj.NODES_MAP))
            {
                synchronized (snap)
                {
                    /*
                     * we do not want this code in the above synchronized(snap) block as we MUST take the more general
                     * locks. However, if we do take the general locks, a blocked port cannot be processed as the
                     * DeviceManager is still holding the general locks.
                     * This way, the blocked port is already processed in the synchronized block above. Additionally
                     * "snapKey" should only be != null in case of success (preCtrlNotifyBackupShipped will upload the
                     * s3 .meta file) so we should not even get into this then-case if a port was blocked
                     */
                    success = preCtrlNotifyBackupShipped(success, restoring, snap, shippingInfo);
                }
            }
            // success = waitForSnapCreateFinished(snap) && success;
            controllerPeerConnector.getControllerPeer().sendMessage(
                interComSerializer.onewayBuilder(internalApiName)
                    .notifyBackupShipped(
                        snapKey,
                        success,
                        portsUsed == null ? new TreeSet<>() : portsUsed,
                        s3orLinRemoteName
                    )
                    .build(),
                internalApiName
            );
        }
        if (shippingInfo != null && done)
        {
            for (SnapVlmDataInfo snapVlmDataInfo : shippingInfo.snapVlmDataInfoMap.values())
            {
                snapVlmDataInfo.daemon.shutdown(false); // just make sure that everything is already stopped
            }
        }
    }

    /**
     * In cases where the shipping finished faster (success irrelevant) than the ctrl can finish the take-snapshot
     * flux-chain (and consequently set SUCCESSFUL), this ensures that the ctrl doesn't get confused
     *
     * @param snap
     *
     * @return
     */
    private boolean waitForSnapCreateFinished(Snapshot snap)
    {
        boolean success = false;
        final int secsToWait = 300;
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        try
        {
            int i = 0;
            for (i = 0; i < secsToWait; i++)
            {
                // assign to new object so the code analyzer doesn't complain about using a method parameter for syncing
                final Object syncObj = snap;
                synchronized (syncObj)
                {
                    if (snap.isDeleted() || snapDfn.isDeleted())
                    {
                        break;
                    }
                    StateFlags<Flags> snapDfnFlags = snapDfn.getFlags();
                    if (snapDfnFlags.isSet(accCtx, SnapshotDefinition.Flags.SUCCESSFUL))
                    {
                        success = true;
                        break;
                    }
                    boolean shipmentFailed = BackupShippingUtils.hasShippingStatus(
                        snapDfn,
                        null,
                        InternalApiConsts.VALUE_PREPARE_ABORT,
                        accCtx
                    );
                    shipmentFailed &= BackupShippingUtils.hasShippingStatus(
                        snapDfn,
                        null,
                        InternalApiConsts.VALUE_ABORTING,
                        accCtx
                    );
                    shipmentFailed &= BackupShippingUtils.hasShippingStatus(
                        snapDfn,
                        null,
                        InternalApiConsts.VALUE_ABORTED,
                        accCtx
                    );
                    shipmentFailed &= BackupShippingUtils.hasShippingStatus(
                        snapDfn,
                        null,
                        InternalApiConsts.VALUE_FAILED,
                        accCtx
                    );
                    if (
                        snap.getFlags().isSet(accCtx, Snapshot.Flags.DELETE) ||
                            snapDfnFlags.isSomeSet(
                                accCtx,
                                SnapshotDefinition.Flags.DELETE,
                                SnapshotDefinition.Flags.FAILED_DEPLOYMENT,
                                SnapshotDefinition.Flags.FAILED_DISCONNECT
                            ) || shipmentFailed
                    )
                    {
                        break;
                    }
                }
                Thread.sleep(1_000);
            }
            if (i == secsToWait)
            {
                errorReporter.logDebug("Snapshot %s was not taken successfully within %d seconds", snap, secsToWait);
            }
        }
        catch (InterruptedException exc)
        {
            errorReporter.reportError(exc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return success;
    }

    protected String fillPojo(Snapshot snap, String basedOnMetaName, ShippingInfo info)
        throws AccessDeniedException, IOException, ParseException
    {
        Map<Integer, List<BackupMetaInfoPojo>> backupsRef = new TreeMap<>();
        for (SnapVlmDataInfo snapInfo : info.snapVlmDataInfoMap.values())
        {
            BackupMetaInfoPojo backInfo = new BackupMetaInfoPojo(
                snapInfo.backupName,
                snapInfo.finishTimestamp,
                snap.getNodeName().displayValue
            );
            List<BackupMetaInfoPojo> list = backupsRef.get(snapInfo.vlmNr);
            if (list == null)
            {
                list = new ArrayList<>();
                backupsRef.put(snapInfo.vlmNr, list);
            }
            list.add(backInfo);
        }

        return BackupShippingUtils.fillPojo(
            accCtx,
            snap,
            stltConfigAccessor.getReadonlyProps(),
            stltSecObj.getEncKey(),
            stltSecObj.getHash(),
            stltSecObj.getSalt(),
            backupsRef,
            basedOnMetaName,
            info.s3orStltRemote.getName().displayValue
        );
    }

    public boolean alreadyStarted(AbsStorageVlmData<Snapshot> snapVlmDataRef, String remoteName)
    {
        synchronized (startedShippments)
        {
            return startedShippments.contains(getShipKey(remoteName, snapVlmDataRef.getVolume().getAbsResource()));
        }
    }

    public void removeSnapFromStartedShipments(String rscName, String snapName, String remoteName)
    {
        PairNonNull<Snapshot, RemoteName> targetKey = null;
        synchronized (startedShippments)
        {
            for (PairNonNull<Snapshot, RemoteName> shipKey : startedShippments)
            {
                Snapshot snap = shipKey.objA;
                if (
                    snap.getResourceName().displayValue.equals(rscName) &&
                        snap.getSnapshotName().displayValue.equals(snapName) &&
                        shipKey.objB.displayValue.equals(remoteName)
                )
                {
                    targetKey = shipKey;
                }
            }
            if (targetKey != null)
            {
                startedShippments.remove(targetKey);
            }
        }
    }

    public boolean alreadyFinished(
        AbsStorageVlmData<Snapshot> snapVlmDataRef,
        String s3orLinRemoteName,
        boolean isTarget
    )
    {
        try
        {
            List<String> list = finishedShipments.get(
                getShipKey(s3orLinRemoteName, snapVlmDataRef.getVolume().getAbsResource())
            );
            String namespc;
            if (isTarget)
            {
                namespc = BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC;
            }
            else
            {
                namespc = BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemoteName;
            }
            return list != null && list.contains(
                snapVlmDataRef.getVolume()
                    .getAbsResource()
                    .getSnapshotDefinition()
                    .getSnapDfnProps(accCtx)
                    .getProp(InternalApiConsts.KEY_BACKUP_TO_RESTORE, namespc)
            );
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public ServiceName getServiceName()
    {
        return serviceName;
    }

    @Override
    public String getServiceInfo()
    {
        return serviceName.displayValue;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return instanceName;
    }

    @Override
    public boolean isStarted()
    {
        return serviceStarted;
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {
        instanceName = instanceNameRef;
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        serviceStarted = true;
    }

    @Override
    public void shutdown(boolean ignoredJvmShutdownRef)
    {
        serviceStarted = false;
        for (ShippingInfo info : shippingInfoMap.values())
        {
            for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
            {
                snapVlmDataInfo.daemon.shutdown(false);
            }
        }

    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        long exitTime = Math.addExact(System.currentTimeMillis(), timeoutRef);
        for (ShippingInfo info : shippingInfoMap.values())
        {
            for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
            {
                long now = System.currentTimeMillis();
                if (now < exitTime)
                {
                    long maxWaitTime = exitTime - now;
                    snapVlmDataInfo.daemon.awaitShutdown(maxWaitTime);
                }
            }
        }
    }

    public void snapshotDeleted(Snapshot snap)
    {
        Set<PairNonNull<Snapshot, RemoteName>> keysToDel = new HashSet<>();
        synchronized (startedShippments)
        {
            for (PairNonNull<Snapshot, RemoteName> shipKey : startedShippments)
            {
                if (shipKey.objA.equals(snap))
                {
                    keysToDel.add(shipKey);
                }
            }
            startedShippments.removeAll(keysToDel);
        }
        keysToDel.clear();
        for (PairNonNull<Snapshot, RemoteName> shipKey : finishedShipments.keySet())
        {
            if (shipKey.objA.equals(snap))
            {
                keysToDel.add(shipKey);
            }
        }
        finishedShipments.keySet().removeAll(keysToDel);
    }

    public void setPrepareAbort(Snapshot snap, String remoteName)
    {
        // no need to sync to snap because when postShipping runs, setting this can't trigger postShipping again
        // (sync(snap) is only to prevent stuff from happening at the same time as postShipping)
        @Nullable ShippingInfo shippingInfo = shippingInfoMap.get(getShipKey(remoteName, snap));
        if (shippingInfo != null)
        {
            synchronized (shippingInfo)
            {
                // if the daemon didn't start running yet, make sure it won't start at a later time either
                shippingInfo.isStarted = true;
                for (SnapVlmDataInfo vlmData : shippingInfo.snapVlmDataInfoMap.values())
                {
                    vlmData.daemon.setPrepareAbort();
                }
            }
        }
    }

    protected abstract BackupShippingDaemon createDaemon(
        AbsStorageVlmData<Snapshot> snapVlmDataRef,
        String[] fullCommandRef,
        String backupNameRef,
        AbsRemote remoteRef,
        boolean restoreRef,
        @Nullable Integer portRef,
        BiConsumer<Boolean, Integer> postActionRef
    );

    protected abstract String getBackupNameForRestore(AbsStorageVlmData<Snapshot> snapVlmDataRef)
        throws InvalidKeyException, AccessDeniedException;

    protected abstract String getCommandSending(
        String cmdRef,
        AbsRemote remoteRef,
        AbsStorageVlmData<Snapshot> snapVlmDataRef
    ) throws AccessDeniedException;

    protected abstract String getCommandReceiving(
        String cmdRef,
        AbsRemote remoteRef,
        AbsStorageVlmData<Snapshot> snapVlmDataRef
    ) throws AccessDeniedException;

    protected abstract boolean preCtrlNotifyBackupShipped(
        boolean successRef,
        boolean restoringRef,
        Snapshot snapRef,
        ShippingInfo shippingInfoRef
    );

    protected abstract void postAllBackupPartsRegistered(Snapshot snap, ShippingInfo info);

    static class ShippingInfo
    {
        private boolean isStarted = false;
        Map<AbsStorageVlmData<Snapshot>, SnapVlmDataInfo> snapVlmDataInfoMap = new HashMap<>();
        @Nullable AbsRemote s3orStltRemote = null;
        Set<Integer> portsUsed = new TreeSet<>();
        Set<Integer> alreadyInUse = new TreeSet<>();

        @Nullable String s3MetaKey;
        @Nullable String basedOnS3MetaKey;

        int snapVlmDataFinishedShipping = 0;
        private int snapVlmDataFinishedSuccessfully = 0;
    }

    static class SnapVlmDataInfo
    {
        private BackupShippingDaemon daemon;
        String backupName;
        int vlmNr;

        private long finishTimestamp;

        private SnapVlmDataInfo(BackupShippingDaemon daemonRef, String backupNameRef, int vlmNrRef)
        {
            daemon = daemonRef;
            backupName = backupNameRef;
            vlmNr = vlmNrRef;
        }
    }
}
