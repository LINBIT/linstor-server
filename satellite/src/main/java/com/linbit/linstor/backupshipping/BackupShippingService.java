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
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.backups.BackupInfoPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.LuksLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.RscDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.RscMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmMetaPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltConnTracker;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.SdkClientException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Singleton
public class BackupShippingService implements SystemService
{
    public static final ServiceName SERVICE_NAME;
    public static final String SERVICE_INFO = "BackupShippingService";
    private static final DateFormat SDF = new SimpleDateFormat("yyyyMMdd_HHmmss");
    private static final String BACKUP_KEY_FORMAT = "%s%s_%05d_%s";
    private static final String CMD_FORMAT_SENDING =
        "trap 'kill -HUP 0' SIGTERM; " +
        "(" +
            "%s | " +  // thin_send prev_LV_snapshot cur_LV_snapshot
            // "pv -s 100m -bnr -i 0.1 | " +
            "zstd;" +
        ")&\\wait $!";

    private static final String CMD_FORMAT_RECEIVING = "trap 'kill -HUP 0' SIGTERM; " +
        "exec 7<&0 0</dev/null; " +
        "set -o pipefail; " +
        "(" +
        "exec 0<&7 7<&-; zstd -d | " +
        // "pv -s 100m -bnr -i 0.1 | " +
        "%s ;" +
        ") & wait $!";

    private final BackupToS3 backupHandler;
    private final Map<Snapshot, ShippingInfo> shippingInfoMap;
    private final Set<Snapshot> startedShippments;
    private final ThreadGroup threadGroup;
    private final AccessContext accCtx;
    private final RemoteMap remoteMap;

    private ServiceName instanceName;
    private boolean serviceStarted = false;
    private ErrorReporter errorReporter;
    private ExtCmdFactory extCmdFactory;
    private ControllerPeerConnector controllerPeerConnector;
    private CtrlStltSerializer interComSerializer;
    private StltSecurityObjects stltSecObj;
    private StltConfigAccessor stltConfigAccessor;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName(SERVICE_INFO);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Inject
    public BackupShippingService(
        BackupToS3 backupHandlerRef,
        ErrorReporter errorReporterRef,
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
        backupHandler = backupHandlerRef;
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        accCtx = accCtxRef;
        stltSecObj = stltSecObjRef;
        stltConfigAccessor = stltConfigAccessorRef;
        remoteMap = remoteMapRef;

        try
        {
            instanceName = new ServiceName(SERVICE_INFO);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        shippingInfoMap = Collections.synchronizedMap(new TreeMap<>());
        startedShippments = Collections.synchronizedSet(new TreeSet<>());
        threadGroup = new ThreadGroup("SnapshotShippingSerivceThreadGroup");

        // this causes all shippings to be aborted should the satellite lose connection to the controller
        stltConnTracker.addClosingListener(this::killAllShipping);
    }

    public void killAllShipping() throws StorageException
    {
        for (ShippingInfo shippingInfo : shippingInfoMap.values())
        {
            for (SnapVlmDataInfo snapVlmDataInfo : shippingInfo.snapVlmDataInfoMap.values())
            {
                snapVlmDataInfo.daemon.shutdown();
            }
        }
        shippingInfoMap.clear();
    }

    public void abort(AbsStorageVlmData<Snapshot> snapVlmData)
    {
        errorReporter.logDebug(
            "aborting backup shipping: %s",
            snapVlmData.getRscLayerObject().getAbsResource().toString()
        );
        Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
        ShippingInfo info = shippingInfoMap.get(snap);
        if (info != null)
        {
            SnapVlmDataInfo snapVlmDataInfo = info.snapVlmDataInfoMap.get(snapVlmData);
            if (snapVlmDataInfo != null)
            {
                snapVlmDataInfo.daemon.shutdown();
            }
        }
        else
        {
            errorReporter.logDebug("  backupShippingInfo is null, nothing to shutdown");
        }
    }

    public void sendBackup(
        String snapNameRef,
        String rscNameRef,
        String rscNameSuffixRef,
        int vlmNrRef,
        String cmdRef,
        AbsStorageVlmData<Snapshot> snapVlmData
    ) throws StorageException, InvalidNameException, InvalidKeyException, AccessDeniedException
    {
        if (!rscNameSuffixRef.equals(RscLayerSuffixes.SUFFIX_DRBD_META))
        {
            String backupName = String.format(BACKUP_KEY_FORMAT, rscNameRef, rscNameSuffixRef, vlmNrRef, snapNameRef);
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
                    String.format(
                        CMD_FORMAT_SENDING,
                        cmdRef
                    )
                },
                snapNameRef,
                backupName,
                remoteName,
                false,
                success -> postShipping(
                    success,
                    snapVlmData,
                    InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_SENT,
                    true,
                    false
                ),
                snapVlmData
            );
        }
    }

    public void restoreBackup(
        String snapNameRef,
        String rscNameRef,
        String rscNameSuffixRef,
        int vlmNrRef,
        String cmdRef,
        AbsStorageVlmData<Snapshot> snapVlmData
    ) throws StorageException, AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        String backupName = "";
        SnapshotVolume snapVlm = (SnapshotVolume) snapVlmData.getVolume();
        String metaName = snapVlm.getSnapshot().getProps(accCtx)
            .getProp(InternalApiConsts.KEY_BACKUP_META_TO_RESTORE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
        Pattern metaPattern = Pattern.compile("^([a-zA-Z0-9_-]{2,48})_(back_[0-9]{8}_[0-9]{6})\\.meta$");
        Matcher m = metaPattern.matcher(metaName);
        if (m.matches())
        {
            backupName = String.format(BACKUP_KEY_FORMAT, m.group(1), rscNameSuffixRef, vlmNrRef, m.group(2));
        }
        else
        {
            throw new ImplementationError("Not a meta file: " + metaName);
        }
        String remoteName = snapVlm.getSnapshot().getProps(accCtx)
            .getProp(InternalApiConsts.KEY_BACKUP_SRC_REMOTE, ApiConsts.NAMESPC_BACKUP_SHIPPING);
        startDaemon(
            cmdRef,
            new String[]
            {
                "setsid",
                "-w",
                "bash",
                "-c",
                String.format(
                    CMD_FORMAT_RECEIVING,
                    cmdRef
                )
            },
            snapNameRef,
            backupName,
            remoteName,
            true,
            success -> postShipping(
                success,
                snapVlmData,
                InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_RECEIVED,
                true,
                true
            ),
            snapVlmData
        );
    }

    public void allBackupPartsRegistered(Snapshot snap)
    {
        synchronized (snap)
        {
            ShippingInfo info = shippingInfoMap.get(snap);
            if (info != null)
            {
                synchronized (info)
                {
                    if (!info.isStarted)
                    {
                        for (SnapVlmDataInfo snapVlmDataInfo : info.snapVlmDataInfoMap.values())
                        {
                            String uploadId = snapVlmDataInfo.daemon.start();
                            startedShippments.add(snap);
                            if (uploadId != null)
                            {
                                controllerPeerConnector.getControllerPeer().sendMessage(
                                    interComSerializer.onewayBuilder(InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_ID).notifyBackupShippingId(snap, snapVlmDataInfo.backupName, uploadId).build()
                                );
                            }
                        }
                        info.isStarted = true;
                    }
                }
            }
        }
    }

    private void startDaemon(
        String sendRecvCommand,
        String[] fullCommand,
        String shippingDescr,
        String backupNameRef,
        String remoteName,
        boolean restore,
        Consumer<Boolean> postAction,
        AbsStorageVlmData<Snapshot> snapVlmData
    )
        throws StorageException, InvalidNameException
    {
        if (serviceStarted)
        {
            if (!alreadyStarted(snapVlmData))
            {
                killIfRunning(sendRecvCommand);
                long size = snapVlmData.getAllocatedSize();
                Remote remote = remoteMap.get(new RemoteName(remoteName));
                if (!(remote instanceof S3Remote))
                {
                    throw new ImplementationError(
                        "Unknown implementation of Remote found: " + remote.getClass().getCanonicalName()
                    );
                }

                BackupShippingDaemon daemon = new BackupShippingDaemon(
                    errorReporter,
                    threadGroup,
                    "shipping_" + shippingDescr,
                    fullCommand,
                    backupNameRef,
                    (S3Remote) remote,
                    backupHandler,
                    restore,
                    size,
                    postAction,
                    accCtx,
                    stltSecObj.getCryptKey()
                );
                Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
                ShippingInfo info = shippingInfoMap.get(snap);
                if (info == null)
                {
                    info = new ShippingInfo();
                    shippingInfoMap.put(snap, info);
                }
                info.snapVlmDataInfoMap.put(snapVlmData, new SnapVlmDataInfo(daemon, backupNameRef));
                info.remote = (S3Remote) remote;
            }
        }
        else
        {
            throw new StorageException("BackupShippingService not started");
        }
    }

    private void postShipping(
        boolean successRef,
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
                if (successRef)
                {
                    shippingInfo.snapVlmDataFinishedSuccessfully++;
                }
                if (shippingInfo.snapVlmDataFinishedShipping == shippingInfo.snapVlmDataInfoMap.size())
                {
                    if (updateCtrlRef)
                    {
                        boolean success = shippingInfo.snapVlmDataFinishedSuccessfully == shippingInfo.snapVlmDataFinishedShipping;
                        if (success && !restoring)
                        {
                            String key;
                            try
                            {
                                key = snap.getResourceName() + "_" + snap.getSnapshotDefinition().getProps(accCtx)
                                    .getProp(
                                        InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                                    ) + ".meta";
                                backupHandler.putObject(
                                    key, fillPojo(snap), shippingInfo.remote, accCtx, stltSecObj.getCryptKey()
                                );
                            }
                            catch (InvalidKeyException | AccessDeniedException | JsonProcessingException exc)
                            {
                                errorReporter.reportError(new ImplementationError(exc));
                                success = false;
                            }
                            catch (SdkClientException exc)
                            {
                                errorReporter.reportError(exc);
                                success = false;
                            }
                        }
                        controllerPeerConnector.getControllerPeer().sendMessage(
                            interComSerializer.onewayBuilder(internalApiName).notifyBackupShipped(snap, success).build()
                        );
                    }

                    for (SnapVlmDataInfo snapVlmDataInfo : shippingInfo.snapVlmDataInfoMap.values())
                    {
                        snapVlmDataInfo.daemon.shutdown(); // just make sure that everything is already stopped
                    }
                    shippingInfoMap.remove(snap);
                }
            }
        }
    }

    private String fillPojo(Snapshot snap) throws JsonProcessingException, AccessDeniedException
    {
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        PriorityProps rscDfnPrio = new PriorityProps(
            snapDfn.getProps(accCtx),
            snapDfn.getResourceDefinition().getResourceGroup().getProps(accCtx),
            snap.getNode().getProps(accCtx),
            stltConfigAccessor.getReadonlyProps()
        );
        Map<String, String> rscDfnPropsRef = rscDfnPrio.renderRelativeMap("");
        rscDfnPropsRef = new TreeMap<>(rscDfnPropsRef);
        long rscDfnFlagsRef = snapDfn.getResourceDefinition().getFlags().getFlagsBits(accCtx);

        Map<Integer, VlmDfnMetaPojo> vlmDfnsRef = new TreeMap<>();
        Collection<SnapshotVolumeDefinition> vlmDfns = snapDfn.getAllSnapshotVolumeDefinitions(accCtx);
        for (SnapshotVolumeDefinition snapVlmDfn : vlmDfns)
        {
            PriorityProps vlmDfnPrio = new PriorityProps(
                snapVlmDfn.getProps(accCtx),
                snapDfn.getResourceDefinition().getResourceGroup()
                    .getVolumeGroupProps(accCtx, snapVlmDfn.getVolumeNumber())
            );
            Map<String, String> vlmDfnPropsRef = vlmDfnPrio.renderRelativeMap("");
            vlmDfnPropsRef = new TreeMap<>(vlmDfnPropsRef);
            // necessary to get the gross-size-flag, even though flags might have changed in the meantime
            long vlmDfnFlagsRef = snapVlmDfn.getVolumeDefinition().getFlags().getFlagsBits(accCtx);
            long sizeRef = snapVlmDfn.getVolumeSize(accCtx);
            vlmDfnsRef
                .put(snapVlmDfn.getVolumeNumber().value, new VlmDfnMetaPojo(vlmDfnPropsRef, vlmDfnFlagsRef, sizeRef));
        }

        RscDfnMetaPojo rscDfnRef = new RscDfnMetaPojo(rscDfnPropsRef, rscDfnFlagsRef, vlmDfnsRef);

        Map<String, String> rscPropsRef = snap.getProps(accCtx).map();
        long rscFlagsRef = 0;

        Map<Integer, VlmMetaPojo> vlmsRef = new TreeMap<>();
        Iterator<SnapshotVolume> vlmIt = snap.iterateVolumes();
        while (vlmIt.hasNext())
        {
            SnapshotVolume vlm = vlmIt.next();
            Map<String, String> vlmPropsRef = vlm.getProps(accCtx).map();
            long vlmFlagsRef = 0;
            vlmsRef.put(vlm.getVolumeNumber().value, new VlmMetaPojo(vlmPropsRef, vlmFlagsRef));
        }

        RscMetaPojo rscRef = new RscMetaPojo(rscPropsRef, rscFlagsRef, vlmsRef);

        // TODO: for inc, get backups from old meta file
        List<List<BackupInfoPojo>> backupsRef = new ArrayList<>();
        String finishedTime = SDF.format(new Date());
        for (SnapVlmDataInfo snapInfo : shippingInfoMap.get(snap).snapVlmDataInfoMap.values())
        {
            BackupInfoPojo backInfo = new BackupInfoPojo(
                snapInfo.backupName,
                finishedTime,
                snap.getNodeName().displayValue
            );
            backupsRef.add(Arrays.asList(backInfo));
        }

        LuksLayerMetaPojo luksPojo = null;
        List<AbsRscLayerObject<Snapshot>> luksLayers = LayerUtils.getChildLayerDataByKind(
            snap.getLayerData(accCtx),
            DeviceLayerKind.LUKS
        );
        if (
            !luksLayers.isEmpty() &&
            stltSecObj.getEncKey() != null && stltSecObj.getHash() != null && stltSecObj.getSalt() != null
        )
        {
            luksPojo = new LuksLayerMetaPojo(
                Base64.encode(stltSecObj.getEncKey()),
                Base64.encode(stltSecObj.getHash()),
                Base64.encode(stltSecObj.getSalt())
            );
        }

        RscLayerDataApi layersRef = snap.getLayerData(accCtx).asPojo(accCtx);

        BackupMetaDataPojo pojo = new BackupMetaDataPojo(layersRef, rscDfnRef, rscRef, luksPojo, backupsRef);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(pojo);
    }

    private boolean alreadyStarted(AbsStorageVlmData<Snapshot> snapVlmDataRef)
    {
        return startedShippments.contains(snapVlmDataRef.getVolume().getAbsResource());
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
                    line = line.trim(); // ps prints a trailing space
                    String pid = line.substring(0, line.indexOf(" "));
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
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
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
                snapVlmDataInfo.daemon.shutdown();
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
        startedShippments.remove(snap);
    }

    private static class ShippingInfo
    {
        private boolean isStarted = false;
        private Map<AbsStorageVlmData<Snapshot>, SnapVlmDataInfo> snapVlmDataInfoMap = new HashMap<>();
        private S3Remote remote = null;

        private int snapVlmDataFinishedShipping = 0;
        private int snapVlmDataFinishedSuccessfully = 0;
    }

    private static class SnapVlmDataInfo
    {
        private BackupShippingDaemon daemon;
        private String backupName;

        private SnapVlmDataInfo(BackupShippingDaemon daemonRef, String backupNameRef)
        {
            daemon = daemonRef;
            backupName = backupNameRef;
        }
    }

}
