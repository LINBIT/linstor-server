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
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.DrbdLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.DrbdLayerMetaPojo.DrbdLayerVlmMetaPojo;
import com.linbit.linstor.api.pojo.backups.LayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.LuksLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.RscDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.RscMetaPojo;
import com.linbit.linstor.api.pojo.backups.StorageLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.StorageLayerMetaPojo.StorageLayerVlmMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmMetaPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import com.amazonaws.SdkClientException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Singleton
public class BackupShippingService implements SystemService
{
    public static final ServiceName SERVICE_NAME;
    public static final String SERVICE_INFO = "BackupShippingService";
    private static final String CMD_FORMAT_SENDING =
        "trap 'kill -HUP 0' SIGTERM; " +
        "(" +
            "%s | " +  // thin_send prev_LV_snapshot cur_LV_snapshot
            // "pv -s 100m -bnr -i 0.1 | " +
            "zstd;" +
        ")&\\wait $!";

    private static final String CMD_FORMAT_RECEIVING = "trap 'kill -HUP 0' SIGTERM; " +
        "set -o pipefail; " +
        "(" +
        "zstd -d | " +
        // "pv -s 100m -bnr -i 0.1 | " +
        "%s ;" +
        ")& wait $!";

    private final BackupToS3 backupHandler;
    private final Map<Snapshot, ShippingInfo> shippingInfoMap;
    private final Set<Snapshot> startedShippments;
    private final ThreadGroup threadGroup;
    private final AccessContext accCtx;

    private ServiceName instanceName;
    private boolean serviceStarted = false;
    private ErrorReporter errorReporter;
    private ExtCmdFactory extCmdFactory;
    private ControllerPeerConnector controllerPeerConnector;
    private CtrlStltSerializer interComSerializer;
    private StltSecurityObjects stltSecObj;
    private WhitelistProps whitelistProps;
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
        WhitelistProps whitelistPropsRef,
        StltConfigAccessor stltConfigAccessorRef
    )
    {
        backupHandler = backupHandlerRef;
        errorReporter = errorReporterRef;
        extCmdFactory = extCmdFactoryRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        interComSerializer = interComSerializerRef;
        accCtx = accCtxRef;
        stltSecObj = stltSecObjRef;
        whitelistProps = whitelistPropsRef;
        stltConfigAccessor = stltConfigAccessorRef;

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
            errorReporter.logDebug("  shippingInfo is null, nothing to shutdown");
        }
    }

    public void sendBackup(
        String snapNameRef,
        String rscNameRef,
        String rscNameSuffixRef,
        int vlmNrRef,
        String cmdRef,
        AbsStorageVlmData<Snapshot> snapVlmData
    ) throws StorageException
    {

        String backupName = rscNameRef + rscNameSuffixRef + "_" + vlmNrRef + "_" + snapNameRef;
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
            success -> postShipping(
                success,
                snapVlmData,
                InternalApiConsts.API_NOTIFY_BACKUP_SHIPPING_RECEIVED,
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
                            snapVlmDataInfo.daemon.start();
                            startedShippments.add(snap);
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
        Consumer<Boolean> postAction,
        AbsStorageVlmData<Snapshot> snapVlmData
    )
        throws StorageException
    {
        if (serviceStarted)
        {
            if (!alreadyStarted(snapVlmData))
            {
                killIfRunning(sendRecvCommand);

                BackupShippingDaemon daemon = new BackupShippingDaemon(
                    errorReporter,
                    threadGroup,
                    "shipping_" + shippingDescr,
                    fullCommand,
                    backupNameRef,
                    backupHandler,
                    postAction
                );
                Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
                ShippingInfo info = shippingInfoMap.get(snap);
                if (info == null)
                {
                    info = new ShippingInfo();
                    shippingInfoMap.put(snap, info);
                }
                info.snapVlmDataInfoMap.put(snapVlmData, new SnapVlmDataInfo(daemon, backupNameRef));
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
        boolean updateCtrlRef
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
                        if (success)
                        {
                            String key;
                            try
                            {
                                key = snap.getResourceName() + "_" + snap.getSnapshotDefinition().getProps(accCtx)
                                    .getProp(
                                        InternalApiConsts.KEY_LAST_FULL_BACKUP_TIMESTAMP,
                                        ApiConsts.NAMESPC_BACKUP_SHIPPING
                                    );
                                backupHandler.putObject(key, fillPojo(snap));
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
        ResourceDefinition rscDfn = snap.getResourceDefinition();
        PriorityProps rscDfnPrio = new PriorityProps(
            rscDfn.getProps(accCtx),
            rscDfn.getResourceGroup().getProps(accCtx),
            snap.getNode().getProps(accCtx),
            stltConfigAccessor.getReadonlyProps()
        );
        Map<String, String> rscDfnPropsRef = rscDfnPrio.renderRelativeMap("");
        Iterator<Entry<String, String>> rscDfnPropsIt = rscDfnPropsRef.entrySet().iterator();
        while (rscDfnPropsIt.hasNext())
        {
            Map.Entry<String, String> entry = rscDfnPropsIt.next();
            if (!whitelistProps.isKeyKnown(LinStorObject.RESOURCE_DEFINITION, entry.getKey()))
            {
                rscDfnPropsIt.remove();
            }
        }
        rscDfnPropsRef = new TreeMap<>(rscDfnPropsRef);
        long rscDfnFlagsRef = rscDfn.getFlags().getFlagsBits(accCtx);

        Map<Integer, VlmDfnMetaPojo> vlmDfnsRef = new TreeMap<>();
        Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn(accCtx);
        while (vlmDfnIt.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIt.next();
            PriorityProps vlmDfnPrio = new PriorityProps(
                vlmDfn.getProps(accCtx),
                rscDfn.getResourceGroup().getVolumeGroupProps(accCtx, vlmDfn.getVolumeNumber())
            );
            Map<String, String> vlmDfnPropsRef = vlmDfnPrio.renderRelativeMap("");
            Iterator<Entry<String, String>> vlmDfnPropsIt = vlmDfnPropsRef.entrySet().iterator();
            while (vlmDfnPropsIt.hasNext())
            {
                Map.Entry<String, String> entry = vlmDfnPropsIt.next();
                if (!whitelistProps.isKeyKnown(LinStorObject.VOLUME_DEFINITION, entry.getKey()))
                {
                    vlmDfnPropsIt.remove();
                }
            }
            vlmDfnPropsRef = new TreeMap<>(vlmDfnPropsRef);
            long vlmDfnFlagsRef = vlmDfn.getFlags().getFlagsBits(accCtx);
            long sizeRef = vlmDfn.getVolumeSize(accCtx);
            vlmDfnsRef.put(vlmDfn.getVolumeNumber().value, new VlmDfnMetaPojo(vlmDfnPropsRef, vlmDfnFlagsRef, sizeRef));
        }

        RscDfnMetaPojo rscDfnRef = new RscDfnMetaPojo(rscDfnPropsRef, rscDfnFlagsRef, vlmDfnsRef);

        Resource rsc = rscDfn.getResource(accCtx, snap.getNodeName());
        Map<String, String> rscPropsRef = rsc.getProps(accCtx).map();
        long rscFlagsRef = rsc.getStateFlags().getFlagsBits(accCtx);

        Map<Integer, VlmMetaPojo> vlmsRef = new TreeMap<>();
        Iterator<Volume> vlmIt = rsc.iterateVolumes();
        while (vlmIt.hasNext())
        {
            Volume vlm = vlmIt.next();
            Map<String, String> vlmPropsRef = vlm.getProps(accCtx).map();
            long vlmFlagsRef = vlm.getFlags().getFlagsBits(accCtx);
            vlmsRef.put(vlm.getVolumeNumber().value, new VlmMetaPojo(vlmPropsRef, vlmFlagsRef));
        }

        RscMetaPojo rscRef = new RscMetaPojo(rscPropsRef, rscFlagsRef, vlmsRef);

        // TODO: for inc, get backups from old meta file
        List<List<String>> backupsRef = new ArrayList<>();
        for (SnapVlmDataInfo snapInfo : shippingInfoMap.get(snap).snapVlmDataInfoMap.values())
        {
            backupsRef.add(Arrays.asList(snapInfo.backupName));
        }

        LayerMetaPojo layersRef = createLayerPojo(rsc.getLayerData(accCtx));

        BackupMetaDataPojo pojo = new BackupMetaDataPojo(layersRef, rscDfnRef, rscRef, backupsRef);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(pojo);
    }

    private LayerMetaPojo createLayerPojo(AbsRscLayerObject<Resource> rscData) throws AccessDeniedException
    {
        String type = rscData.getLayerKind().name().toLowerCase();
        List<LayerMetaPojo> children = new ArrayList<>();
        DrbdLayerMetaPojo drbdPojo = null;
        LuksLayerMetaPojo luksPojo = null;
        LayerMetaPojo cachePojo = null;
        LayerMetaPojo writecachePojo = null;
        StorageLayerMetaPojo storPojo = null;
        LayerMetaPojo nvmePojo = null;

        switch(rscData.getLayerKind()) {
            case DRBD:
                DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscData;
                DrbdRscDfnData<Resource> drbdRscDfnData = drbdRscData.getRscDfnLayerObject();
                Map<Integer, DrbdLayerVlmMetaPojo> vlmsMap = new TreeMap<>();
                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    StorPool storPool = drbdVlmData.getExternalMetaDataStorPool();
                    vlmsMap.put(
                        drbdVlmData.getVlmNr().value,
                        new DrbdLayerVlmMetaPojo(
                            drbdVlmData.getVlmNr().value,
                            storPool == null ? null : storPool.getName().displayValue
                        )
                    );
                }
                drbdPojo = new DrbdLayerMetaPojo(
                    drbdRscDfnData.getPeerSlots(),
                    drbdRscDfnData.getAlStripes(),
                    drbdRscDfnData.getAlStripeSize(),
                    drbdRscData.getPeerSlots(),
                    drbdRscData.getAlStripes(),
                    drbdRscData.getAlStripeSize(),
                    drbdRscData.getNodeId().value,
                    drbdRscData.getFlags().getFlagsBits(accCtx),
                    vlmsMap
                );
                break;
            case LUKS:
                LuksRscData<Resource> luksRscData = (LuksRscData<Resource>) rscData;
                Map<Integer, String> vlmPwds = new TreeMap<>();
                for (LuksVlmData<Resource> luksVlmData : luksRscData.getVlmLayerObjects().values())
                {
                    vlmPwds.put(luksVlmData.getVlmNr().value, Base64.encode(luksVlmData.getEncryptedKey()));
                }
                luksPojo = new LuksLayerMetaPojo(
                    Base64.encode(stltSecObj.getEncKey()),
                    Base64.encode(stltSecObj.getHash()),
                    Base64.encode(stltSecObj.getSalt()),
                    vlmPwds
                );
                break;
            case STORAGE:
                StorageRscData<Resource> storRscData = (StorageRscData<Resource>) rscData;
                Map<Integer, StorageLayerVlmMetaPojo> storVlmsMap = new TreeMap<>();
                for (VlmProviderObject<Resource> storVlmData : storRscData.getVlmLayerObjects().values())
                {
                    StorPool storPool = storVlmData.getStorPool();
                    storVlmsMap.put(
                        storVlmData.getVlmNr().value,
                        new StorageLayerVlmMetaPojo(
                            storVlmData.getVlmNr().value,
                            storPool == null ? null : storPool.getName().displayValue,
                            storVlmData.getProviderKind().name().toLowerCase()
                        )
                    );
                }
                storPojo = new StorageLayerMetaPojo(storVlmsMap);
                break;
            case NVME: // no special data
                break;
            case OPENFLEX: // no special data
                break;
            case CACHE: // no special data
                break;
            case WRITECACHE: // no special data
                break;
            case BCACHE: // no special data
                break;
            default:
                throw new ImplementationError("Unknown layer type: " + rscData.getLayerKind().name());
        }

        for (AbsRscLayerObject<Resource> child : rscData.getChildren())
        {
            children.add(createLayerPojo(child));
        }

        return new LayerMetaPojo(
            type,
            rscData.getResourceNameSuffix(),
            drbdPojo,
            luksPojo,
            cachePojo,
            writecachePojo,
            storPojo,
            nvmePojo,
            children
        );
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
