package com.linbit.linstor.backupshipping;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
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
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.Remote.RemoteType;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StltRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;

import javax.inject.Inject;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

public abstract class AbsBackupShippingService implements SystemService
{
    public final ServiceName serviceName;

    private final RemoteType remoteType;
    protected final Map<Snapshot, ShippingInfo> shippingInfoMap;
    private final Set<Snapshot> startedShippments;
    private final Map<Snapshot, List<String>> finishedShipments;
    protected final ThreadGroup threadGroup;
    protected final AccessContext accCtx;
    protected final RemoteMap remoteMap;

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
        RemoteMap remoteMapRef
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
        for (ShippingInfo shippingInfo : shippingInfoMap.values())
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

    public void abort(AbsStorageVlmData<Snapshot> snapVlmData)
    {
        errorReporter.logDebug(
            "[%s] aborting backup shipping: %s",
            remoteType.name(),
            snapVlmData.getRscLayerObject().getAbsResource().toString()
        );
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        ShippingInfo info = shippingInfoMap.get(snap);
        if (info != null)
        {
            SnapVlmDataInfo snapVlmDataInfo = info.snapVlmDataInfoMap.get(snapVlmData);
            if (snapVlmDataInfo != null)
            {
                snapVlmDataInfo.daemon.shutdown(false);
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

    public void sendBackup(
        String snapNameRef,
        String rscNameRef,
        String rscNameSuffixRef,
        int vlmNrRef,
        String cmdRef,
        AbsStorageVlmData<Snapshot> basedOnSnapVlmData,
        AbsStorageVlmData<Snapshot> snapVlmData
    ) throws StorageException, InvalidNameException, InvalidKeyException, AccessDeniedException
    {
        if (RscLayerSuffixes.shouldSuffixBeShipped(rscNameSuffixRef))
        {
            String backupName = BackupShippingUtils.buildS3VolumeName(
                rscNameRef,
                rscNameSuffixRef,
                vlmNrRef,
                snapNameRef,
                stltConfigAccessor.getReadonlyProps().getProp(
                    ApiConsts.KEY_BACKUP_S3_SUFFIX,
                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                )
            );
            String remoteName = ((SnapshotVolume) snapVlmData.getVolume()).getSnapshot().getProps(accCtx)
                .getProp(InternalApiConsts.KEY_BACKUP_TARGET_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
            startDaemon(
                cmdRef,
                new String[]
                {
                    "setsid",
                    "-w",
                    "bash",
                    "-c",
                    getCommandSending(
                        cmdRef,
                        remoteMap.get(
                            new RemoteName(
                                snapVlmData.getRscLayerObject().getAbsResource().getProps(accCtx).getProp(
                                    InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                                    ApiConsts.NAMESPC_BACKUP_SHIPPING
                                ),
                                true
                            )
                        ),
                        snapVlmData
                    )
                },
                snapNameRef,
                backupName,
                remoteMap.get(new RemoteName(remoteName, true)),
                false,
                null,
                (success, addrInUse) -> postShipping(
                    success,
                    addrInUse,
                    snapVlmData,
                    InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_SENT,
                    true,
                    false
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
            String remoteName = snapVlm.getSnapshot().getProps(accCtx).getProp(
                InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                ApiConsts.NAMESPC_BACKUP_SHIPPING
            );
            Remote remote = remoteMap.get(new RemoteName(remoteName, true));

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
                    "setsid",
                    "-w",
                    "bash",
                    "-c",
                    getCommandReceiving(cmdRef, remote, snapVlmData)
                },
                snapVlm.getSnapshotName().displayValue,
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
                    true
                ),
                null,
                snapVlmData
            );
        }
    }

    public void allBackupPartsRegistered(Snapshot snap)
    {
        ShippingInfo info = null;
        boolean justStarted = false;
        synchronized (snap)
        {
            info = shippingInfoMap.get(snap);
            if (info != null)
            {
                synchronized (info)
                {
                    if (!info.isStarted)
                    {
                        String remoteName = "";
                        for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
                        {
                            String uploadId = snapVlmDataInfo.daemon.start();
                            synchronized (startedShippments)
                            {
                                startedShippments.add(snap);
                            }

                            if (uploadId != null)
                            {
                                try
                                {
                                    remoteName = snap.getProps(accCtx).getProp(
                                        InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                                    );
                                    if (remoteName == null || remoteName.isEmpty())
                                    {
                                        throw new ImplementationError(
                                            "KEY_BACKUP_TARGET_REMOTE is not set for the backup-shipping of " +
                                                snap.getSnapshotName()
                                        );
                                    }
                                    controllerPeerConnector.getControllerPeer().sendMessage(
                                        interComSerializer
                                            .onewayBuilder(InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_ID)
                                            .notifyBackupShippingId(
                                                snap,
                                                snapVlmDataInfo.backupName,
                                                uploadId,
                                                remoteName
                                            )
                                            .build()
                                    );
                                }
                                catch (InvalidKeyException | AccessDeniedException exc)
                                {
                                    throw new ImplementationError(exc);
                                }
                            }
                        }
                        justStarted = true;
                        info.isStarted = true;
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
        String shippingDescr,
        String backupNameRef,
        Remote remote,
        boolean restore,
        Integer portRef,
        BiConsumer<Boolean, Integer> postAction,
        AbsStorageVlmData<Snapshot> basedOnSnapVlmData,
        AbsStorageVlmData<Snapshot> snapVlmData
    )
        throws StorageException, InvalidNameException, AccessDeniedException
    {
        ensureRemoteType(remote);

        if (serviceStarted)
        {
            if (!alreadyStarted(snapVlmData))
            {
                killIfRunning(sendRecvCommand);

                Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
                ShippingInfo info = shippingInfoMap.get(snap);
                if (info == null)
                {
                    info = new ShippingInfo();
                    shippingInfoMap.put(snap, info);
                }
                if (remote instanceof StltRemote)
                {
                    info.portsUsed.add(
                        ((StltRemote) remote).getPorts(accCtx)
                            .get(snapVlmData.getVlmNr() + snapVlmData.getRscLayerObject().getResourceNameSuffix())
                    );
                }
                info.snapVlmDataInfoMap.put(
                    snapVlmData,
                    new SnapVlmDataInfo(
                        createDaemon(
                            snapVlmData,
                            shippingDescr,
                            fullCommand,
                            backupNameRef,
                            remote,
                            restore,
                            portRef,
                            postAction
                        ),
                        backupNameRef,
                        snapVlmData.getVlmNr().value
                    )
                );
                info.remote = remote;
                try
                {
                    String s3Suffix = stltConfigAccessor.getReadonlyProps().getProp(
                        ApiConsts.KEY_BACKUP_S3_SUFFIX,
                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                    );

                    info.s3MetaKey = BackupShippingUtils.buildS3MetaKey(snap, s3Suffix);
                    if (basedOnSnapVlmData != null && basedOnSnapVlmData != snapVlmData)
                    {
                        Snapshot basedOnSnap = basedOnSnapVlmData.getRscLayerObject().getAbsResource();
                        String basedOnSnapSuffix = basedOnSnap.getSnapshotDefinition().getProps(accCtx).getProp(
                            ApiConsts.KEY_BACKUP_S3_SUFFIX,
                            ApiConsts.NAMESPC_BACKUP_SHIPPING
                        );
                        info.basedOnS3MetaKey = BackupShippingUtils.buildS3MetaKey(basedOnSnap, basedOnSnapSuffix);
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

    /**
     * Throws an {@link ImplementationError} if <code>this.remoteType</code> does not equals to the parameter's
     * <code>remote.getType()</code>
     *
     * @param remote
     *
     * @throws ImplementationError
     */
    private void ensureRemoteType(Remote remote) throws ImplementationError
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
        boolean restoring
    )
    {
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        synchronized (snap)
        {
            ShippingInfo shippingInfo = shippingInfoMap.get(snap);
            /*
             * shippingInfo might be already null as we delete it at the end of this method.
             */
            if (shippingInfo != null)
            {
                shippingInfo.snapVlmDataFinishedShipping++;
                if (portInUseRef != null)
                {
                    if (shippingInfo.alreadyInUse == null)
                    {
                        shippingInfo.alreadyInUse = new TreeSet<>();
                    }
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
                        if(!shippingInfo.alreadyInUse.isEmpty())
                        {
                            controllerPeerConnector.getControllerPeer().sendMessage(
                                interComSerializer.onewayBuilder(
                                    InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_WRONG_PORTS
                                ).notifyBackupShippingWrongPorts(
                                    shippingInfo.remote.getName().displayValue,
                                    snap.getSnapshotName().displayValue,
                                    snap.getResourceName().displayValue,
                                    shippingInfo.alreadyInUse
                                ).build()
                            );
                        }
                        else
                        {
                            boolean success = shippingInfo.snapVlmDataFinishedSuccessfully ==
                                shippingInfo.snapVlmDataFinishedShipping;

                            success = preCtrlNotifyBackupShipped(success, restoring, snap, shippingInfo);

                            controllerPeerConnector.getControllerPeer().sendMessage(
                                interComSerializer.onewayBuilder(internalApiName)
                                    .notifyBackupShipped(
                                        snap,
                                        success,
                                        shippingInfo.portsUsed == null ? new TreeSet<>() : shippingInfo.portsUsed
                                    )
                                    .build()
                            );
                        }
                    }

                    for (SnapVlmDataInfo snapVlmDataInfo : shippingInfo.snapVlmDataInfoMap.values())
                    {
                        snapVlmDataInfo.daemon.shutdown(false); // just make sure that everything is already stopped
                    }
                    if (shippingInfo.alreadyInUse.isEmpty())
                    {
                        try
                        {
                            String simpleBackupName = snap.getProps(accCtx)
                                .getProp(InternalApiConsts.KEY_BACKUP_TO_RESTORE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                            if (finishedShipments.containsKey(snap))
                            {
                                finishedShipments.get(snap).add(simpleBackupName);
                            }
                            else
                            {
                                finishedShipments.put(snap, new ArrayList<>(Arrays.asList(simpleBackupName)));
                            }
                        }
                        catch (InvalidKeyException | AccessDeniedException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                    }
                    shippingInfoMap.remove(snap);
                }
            }
        }
    }

    protected String fillPojo(Snapshot snap, String basedOnMetaName)
        throws AccessDeniedException, IOException, ParseException
    {
        Map<Integer, List<BackupMetaInfoPojo>> backupsRef = new TreeMap<>();
        for (SnapVlmDataInfo snapInfo : shippingInfoMap.get(snap).snapVlmDataInfoMap.values())
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
            basedOnMetaName
        );
    }

    public boolean alreadyStarted(AbsStorageVlmData<Snapshot> snapVlmDataRef)
    {
        synchronized (startedShippments)
        {
            return startedShippments.contains(snapVlmDataRef.getVolume().getAbsResource());
        }
    }

    public void removeSnapFromStartedShipments(String rscName, String snapName)
    {
        Snapshot target = null;
        synchronized (startedShippments)
        {
            for (Snapshot snap : startedShippments)
            {
                if (
                    snap.getResourceName().displayValue.equals(rscName) &&
                        snap.getSnapshotName().displayValue.equals(snapName)
                )
                {
                    target = snap;
                }
            }
            if (target != null)
            {
                startedShippments.remove(target);
            }
        }
    }

    public boolean alreadyFinished(AbsStorageVlmData<Snapshot> snapVlmDataRef)
    {
        try
        {
            List<String> list = finishedShipments.get(snapVlmDataRef.getVolume().getAbsResource());
            return list != null && list.contains(
                snapVlmDataRef.getVolume().getAbsResource().getProps(accCtx)
                    .getProp(InternalApiConsts.KEY_BACKUP_TO_RESTORE, ApiConsts.NAMESPC_BACKUP_SHIPPING)
            );
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void killIfRunning(String cmdToKill) throws StorageException
    {
        try
        {
            OutputData outputData = extCmdFactory.create().exec(
                "bash",
                "-c",
                "ps ax -o pid,command | grep -E '" + cmdToKill + "' | grep -v grep"
            );
            if (outputData.exitCode == 0) // != 0 means grep didnt find anything
            {
                String out = new String(outputData.stdoutData);
                String[] lines = out.split("\n");
                for (String line : lines)
                {
                    final String lineTrimmed = line.trim(); // ps prints a trailing space
                    String pid = lineTrimmed.substring(0, lineTrimmed.indexOf(" "));
                    extCmdFactory.create().exec("pkill", "-9", "--parent", pid);
                    // extCmdFactory.create().exec("kill", pid);
                }
                Thread.sleep(500); // wait a bit so not just the process is killed but also the socket is closed
            }
        }
        catch (ChildProcessTimeoutException | IOException | InterruptedException exc)
        {
            throw new StorageException("Failed to determine if command is still running: " + cmdToKill, exc);
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
    public void shutdown()
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
        synchronized (startedShippments)
        {
            startedShippments.remove(snap);
        }
        finishedShipments.remove(snap);
    }

    protected abstract BackupShippingDaemon createDaemon(
        AbsStorageVlmData<Snapshot> snapVlmDataRef,
        String shippingDescrRef,
        String[] fullCommandRef,
        String backupNameRef,
        Remote remoteRef,
        boolean restoreRef,
        Integer portRef,
        BiConsumer<Boolean, Integer> postActionRef
    );

    protected abstract String getBackupNameForRestore(AbsStorageVlmData<Snapshot> snapVlmDataRef)
        throws InvalidKeyException, AccessDeniedException;

    protected abstract String getCommandSending(
        String cmdRef,
        Remote remoteRef,
        AbsStorageVlmData<Snapshot> snapVlmDataRef
    ) throws AccessDeniedException;

    protected abstract String getCommandReceiving(
        String cmdRef,
        Remote remoteRef,
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
        Remote remote = null;
        Set<Integer> portsUsed = new TreeSet<>();
        Set<Integer> alreadyInUse = new TreeSet<>();

        String s3MetaKey;
        String basedOnS3MetaKey;

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
