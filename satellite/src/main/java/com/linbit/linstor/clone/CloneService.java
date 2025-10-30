package com.linbit.linstor.clone;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Singleton
public class CloneService implements SystemService
{

    public static final ServiceName SERVICE_NAME;
    public static final String SERVICE_INFO = "CloneService";

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

    private final ErrorReporter errorReporter;
    private final ControllerPeerConnector controllerPeerConnector;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final VolumeDiskStateEvent volumeDiskStateEvent;
    private final ExtCmdFactory extCmdFactory;

    private final ServiceName instanceName;
    private final ConcurrentSkipListSet<CloneInfo> activeClones;
    private final ThreadGroup threadGroup;
    private final ReadWriteLock reconfigurationLock;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final AccessContext sysCtx;

    private boolean serviceStarted = false;

    @Inject
    CloneService(
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        VolumeDiskStateEvent volumeDiskStateEventRef,
        ExtCmdFactory extCmdFactoryRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Provider<DeviceHandler> resourceProcessorRef,
        @SystemContext AccessContext sysCtxRef
    )
    {
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;
        extCmdFactory = extCmdFactoryRef;
        reconfigurationLock = reconfigurationLockRef;
        resourceProcessorProvider = resourceProcessorRef;
        sysCtx = sysCtxRef;

        try
        {
            instanceName = new ServiceName(SERVICE_INFO);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        activeClones = new ConcurrentSkipListSet<>();
        threadGroup = new ThreadGroup("CloneThreadGroup");
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {
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
        for (CloneInfo cloneInfo : activeClones)
        {
            cloneInfo.getCloneDaemon().shutdown();
            cleanupDevices(cloneInfo);
        }
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        long exitTime = Math.addExact(System.currentTimeMillis(), timeout);
        for (CloneInfo cloneInfo : activeClones)
        {
            long now = System.currentTimeMillis();
            if (now < exitTime)
            {
                long maxWaitTime = exitTime - now;
                cloneInfo.getCloneDaemon().awaitShutdown(maxWaitTime);
            }
            cleanupDevices(cloneInfo);
        }
    }

    private void doLVMCloneSourceCleanup(LvmData<Resource> srcData, String cloneName) throws StorageException
    {
        final String srcFullSnapshotName = AbsStorageProvider.getLVMCloneSnapshotNameFull(
            srcData.getIdentifier(), cloneName, srcData.getVlmNr().value
        );

        final String vlmGroup = srcData.getVolumeGroup();
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmGroup),
            config -> LvmCommands.delete(
                extCmdFactory.create(),
                vlmGroup,
                srcFullSnapshotName,
                config,
                LvmCommands.LvmVolumeType.SNAPSHOT
            )
        );
        LvmUtils.recacheNext();
    }

    public void doZFSCloneSourceCleanup(ZfsData<Resource> srcData, String cloneName, boolean zfsClone)
        throws StorageException
    {
        // we cannot delete the snapshot if we have a dependent clone from it
        // so only delete if it was a send/recv clone
        // the snapshot will be tried to delete if the cloned resource gets removed
        if (!zfsClone)
        {
            final String srcFullSnapshotName = AbsStorageProvider.getZFSCloneSnapshotNameFull(
                srcData.getIdentifier(),
                cloneName,
                srcData.getVlmNr().value
            );

            ZfsUtils.deleteIfExists(
                extCmdFactory.create(),
                srcData.getZPool(),
                srcFullSnapshotName,
                ZfsCommands.ZfsVolumeType.SNAPSHOT
            );
        }
    }

    public void doZFSCloneTargetCleanup(ZfsData<Resource> tgtData, boolean zfsClone)
        throws StorageException
    {
        // we cannot delete the snapshot if we have a dependent clone from it
        // so only delete if it was a send/recv clone
        // the snapshot will be tried to delete if the cloned resource gets removed
        if (!zfsClone)
        {
            ZfsUtils.deleteIfExists(
                extCmdFactory.create(),
                tgtData.getZPool(),
                tgtData.getIdentifier() + "@%", // all snapshots of the just cloned volume
                ZfsCommands.ZfsVolumeType.SNAPSHOT
            );
        }
    }

    private void cleanupDevices(CloneInfo cloneInfo)
    {
        Lock writeLock = reconfigurationLock.writeLock();
        try
        {
            writeLock.lock();
            VlmProviderObject<Resource> srcVlmData = cloneInfo.srcVlmData;
            VlmProviderObject<Resource> tgtVlmData = cloneInfo.dstVlmData;
            AbsRscLayerObject<Resource> srcRscData = srcVlmData.getRscLayerObject();
            AbsRscLayerObject<Resource> tgtRscData = tgtVlmData.getRscLayerObject();

            resourceProcessorProvider.get().processAfterClone(
                srcVlmData,
                // use root vlm data to traverse all rsc layers
                tgtVlmData.getRscLayerObject()
                    .getAbsResource()
                    .getLayerData(sysCtx)
                    .getVlmProviderObject(tgtVlmData.getVlmNr()),
                tgtVlmData.getCloneDevicePath()
            );

            resourceProcessorProvider.get().closeAfterClone(
                srcVlmData,
                cloneInfo.getResourceName().displayValue
            );
            resourceProcessorProvider.get().closeAfterClone(tgtVlmData, null);

            // reset passThroughMode
            // "clone passthrough mode" is based on targetRscData for both, srcRscData and targetRscData
            srcRscData.removeClonePassthroughMode(tgtRscData);
            tgtRscData.removeClonePassthroughMode(tgtRscData);

            // cleanup all clone snapshots
            Set<AbsRscLayerObject<Resource>> srcStorRscDataSet = LayerRscUtils.getRscDataByLayer(
                srcRscData,
                DeviceLayerKind.STORAGE
            );

            for (var srcStorRscData : srcStorRscDataSet)
            {
                VlmProviderObject<Resource> vlmObj = srcStorRscData.getVlmLayerObjects().get(srcVlmData.getVlmNr());
                DeviceProviderKind storPoolKind = vlmObj.getProviderKind();
                switch (storPoolKind)
                {
                    case LVM:
                    case LVM_THIN:
                        doLVMCloneSourceCleanup((LvmData<Resource>) vlmObj, cloneInfo.getResourceName().displayValue);
                        break;
                    case ZFS:
                    case ZFS_THIN:
                        doZFSCloneSourceCleanup(
                            (ZfsData<Resource>) vlmObj,
                            cloneInfo.getResourceName().displayValue,
                            cloneInfo.cloneStrategy == DeviceHandler.CloneStrategy.ZFS_CLONE);
                        break;
                    case DISKLESS:
                    case FILE:
                    case SPDK:
                    case EBS_INIT:
                    case FILE_THIN:
                    case EBS_TARGET:
                    case REMOTE_SPDK:
                    case STORAGE_SPACES:
                    case STORAGE_SPACES_THIN:
                    case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                    default:
                        // no cleanup
                        break;

                }
            }
            Set<AbsRscLayerObject<Resource>> tgtStorRscDataSet = LayerRscUtils.getRscDataByLayer(
                tgtRscData,
                DeviceLayerKind.STORAGE
            );
            for (var tgtStorRscData : tgtStorRscDataSet)
            {
                VlmProviderObject<Resource> vlmObj = tgtStorRscData.getVlmLayerObjects().get(tgtVlmData.getVlmNr());
                DeviceProviderKind storPoolKind = vlmObj.getProviderKind();
                switch (storPoolKind)
                {
                    case LVM:
                    case LVM_THIN:
                        // noop (for now?)
                        break;
                    case ZFS:
                    case ZFS_THIN:
                        doZFSCloneTargetCleanup(
                            (ZfsData<Resource>) vlmObj,
                            cloneInfo.cloneStrategy == DeviceHandler.CloneStrategy.ZFS_CLONE
                        );
                        break;
                    case DISKLESS:
                    case FILE:
                    case SPDK:
                    case EBS_INIT:
                    case FILE_THIN:
                    case EBS_TARGET:
                    case REMOTE_SPDK:
                    case STORAGE_SPACES:
                    case STORAGE_SPACES_THIN:
                    case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
                    default:
                        // no cleanup
                        break;

                }
            }
        }
        catch (AccessDeniedException | StorageException exc)
        {
            errorReporter.reportError(exc);
        }
        finally
        {
            writeLock.unlock();
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

    public boolean isRunning(ResourceName rscName, VolumeNumber vlmNr, String suffix)
    {
        return activeClones.stream().anyMatch(cloneInfo ->
            cloneInfo.getResourceName().equals(rscName) && cloneInfo.getVlmNr().equals(vlmNr) &&
                cloneInfo.getSuffix().equals(suffix));
    }

    @SuppressFBWarnings(
        {
            "JLM_JSR166_UTILCONCURRENT_MONITORENTER", "BC_UNCONFIRMED_CAST"
        }
    )
    public void startClone(
        VlmProviderObject<Resource> srcVlmData,
        VlmProviderObject<Resource> dstVlmData,
        DeviceHandler.CloneStrategy strat
    ) throws StorageException
    {
        final CloneInfo cloneInfo = new CloneInfo(
            srcVlmData,
            dstVlmData,
            strat
        );
        if (isStarted())
        {
            // suppress the warning that the syncObject is a concurrent object and therefore has its own sync-methods
            synchronized (activeClones)
            {
                if (!activeClones.contains(cloneInfo))
                {
                    volumeDiskStateEvent.get().triggerEvent(
                        ObjectIdentifier.volumeDefinition(cloneInfo.getResourceName(), cloneInfo.getVlmNr()),
                        "Cloning");

                    activeClones.add(cloneInfo);

                    @Nullable String[] cmd;
                    switch (strat)
                    {
                        case LVM_THIN_CLONE:
                        {
                            LvmThinData<Resource> srcLvmData = (LvmThinData<Resource>) srcVlmData;
                            LvmThinData<Resource> dstLvmData = (LvmThinData<Resource>) dstVlmData;
                            cmd = doLvmThinClone(
                                dstLvmData.getVolumeGroup(), srcLvmData.getIdentifier(), dstLvmData.getIdentifier());
                        }
                        break;
                        case ZFS_CLONE:
                        {
                            ZfsData<Resource> srcZfsData = (ZfsData<Resource>) srcVlmData;
                            ZfsData<Resource> dstZfsData = (ZfsData<Resource>) dstVlmData;
                            final String srcFullSnapshotName = AbsStorageProvider.getZFSCloneSnapshotNameFull(
                                srcZfsData.getIdentifier(),
                                cloneInfo.getResourceName().displayValue,
                                srcZfsData.getVlmNr().value);
                            cmd = doZFSClone(
                                dstZfsData.getZPool(),
                                srcFullSnapshotName,
                                dstZfsData.getIdentifier());
                        }
                            break;
                        case ZFS_COPY:
                        {
                            ZfsData<Resource> srcZfsData = (ZfsData<Resource>) srcVlmData;
                            ZfsData<Resource> dstZfsData = (ZfsData<Resource>) dstVlmData;
                            final String srcFullSnapshotName = AbsStorageProvider.getZFSCloneSnapshotNameFull(
                                srcZfsData.getIdentifier(),
                                cloneInfo.getResourceName().displayValue,
                                srcZfsData.getVlmNr().value);
                            cmd = doZFSCopy(
                                srcZfsData.getZPool(),
                                srcFullSnapshotName,
                                dstZfsData.getZPool(),
                                dstZfsData.getIdentifier());
                        }
                            break;
                        case DD:
                        default:
                        {
                            var ddConv = new ArrayList<>();
                            ddConv.add("nocreat");
                            if (dstVlmData.getProviderKind().usesThinProvisioning())
                            {
                                ddConv.add("sparse");
                            }
                            cmd = new String[]
                                {
                                    "dd",
                                    "if=" + srcVlmData.getCloneDevicePath(),
                                    "of=" + dstVlmData.getCloneDevicePath(),
                                    "bs=64k", // According to the internet this seems currently to be a better default
                                    "conv=" + StringUtils.join(ddConv, ","),
                                    "oflag=direct"
                                };
                        }
                            break;
                    }

                    CloneDaemon cloneDaemon = new CloneDaemon(
                        errorReporter,
                        threadGroup,
                        "clone_" + cloneInfo,
                        cmd,
                        success -> postClone(
                            success,
                            cloneInfo
                        )
                    );
                    cloneInfo.setCloneDaemon(cloneDaemon);
                    cloneDaemon.start();
                }
            }
        }
        else
        {
            throw new StorageException("CloneService not started");
        }
    }

    /**
     * removeClone is called after CLONE_FINISHED is set on the volume.
     * It will remove all clones for this volume, but it is only important to not start a clone a second time.
     * @param rscName resource name
     * @param vlmNr volume number
     */
    public void removeClone(ResourceName rscName, VolumeNumber vlmNr)
    {
        synchronized (activeClones)
        {
            activeClones.removeIf(cloneInfo ->
                cloneInfo.getResourceName().equals(rscName) && cloneInfo.getVlmNr().equals(vlmNr));
        }
    }

    public void setFailed(ResourceName rscName, VolumeNumber vlmNr, String suffix)
    {
        synchronized (activeClones)
        {
            activeClones.forEach(ci ->
            {
                if (ci.getResourceName().equals(rscName) &&
                    ci.getVlmNr().equals(vlmNr) &&
                    ci.getSuffix().equals(suffix))
                {
                    ci.setCloneStatus(false);
                }
            });
        }
    }

    private @Nullable String[] doLvmThinClone(String vlmGroup, String srcSnapshotName, String dstLVName)
        throws StorageException
    {
        // restore
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmGroup),
            config -> LvmCommands.restoreFromSnapshot(
                extCmdFactory.create(),
                srcSnapshotName,
                vlmGroup,
                dstLVName,
                config
            )
        );

        // activate
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmGroup),
            config -> LvmCommands.activateVolume(
                extCmdFactory.create(),
                vlmGroup,
                dstLVName,
                config
            )
        );
        LvmUtils.recacheNextLvs();

        return null;
    }

    private @Nullable String[] doZFSClone(String zpoolName, String srcIdentifier, String dstIdentifier)
        throws StorageException
    {
        ZfsCommands.restoreSnapshot(
            extCmdFactory.create(),
            zpoolName,
            srcIdentifier,
            dstIdentifier
        );
        return null;
    }

    private String[] doZFSCopy(String srcZPoolName, String srcIdentifier, String dstZPoolName, String dstIdentifier)
    {
        return new String[]
            {
                "timeout",
                "0",
                "bash",
                "-c",
                String.format(
                        "set -o pipefail; " +
                        "zfs send --embed --large-block %s/%s | " +
                        "zfs receive -F %s/%s",
                    srcZPoolName,
                    srcIdentifier, // usually snapshot name
                    dstZPoolName,
                    dstIdentifier,
                    dstZPoolName,
                    dstIdentifier)
            };
    }

    /**
     * Returns all clones belonging to the same resource/volume
     *
     * @param cloneInfo
     * @return
     */
    private Set<CloneInfo> volumeClones(final CloneInfo cloneInfo)
    {
        return activeClones.stream()
            .filter(ci -> ci.getResourceName().equals(
                cloneInfo.getResourceName()) && ci.getVlmNr().equals(cloneInfo.getVlmNr()))
            .collect(Collectors.toSet());
    }

    public void notifyCloneStatus(ResourceName rscName, VolumeNumber vlmNr, boolean anyFailed)
    {
        volumeDiskStateEvent.get().triggerEvent(
            ObjectIdentifier.volumeDefinition(rscName, vlmNr),
            anyFailed ? "CloningFailed" : "CloningDone");

        controllerPeerConnector.getControllerPeer().sendMessage(
            ctrlStltSerializer.onewayBuilder(InternalApiConsts.API_NOTIFY_CLONE_UPDATE)
                .notifyCloneUpdate(
                    rscName.getDisplayName(),
                    vlmNr.value,
                    !anyFailed)
                .build(),
            InternalApiConsts.API_NOTIFY_CLONE_UPDATE
        );
    }

    @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    private void postClone(
        boolean successRef,
        CloneInfo cloneInfo
    )
    {
        cleanupDevices(cloneInfo);

        // suppress the warning that the syncObject is a concurrent object and therefore has its own sync-methods
        synchronized (activeClones)
        {
            cloneInfo.setCloneStatus(successRef); // do not move this out of the synchronized block
            Set<CloneInfo> volClones = volumeClones(cloneInfo);
            boolean allDone = volClones.stream().allMatch(ci -> ci.getStatus() == CloneInfo.CloneStatus.FINISH ||
                ci.getStatus() == CloneInfo.CloneStatus.FAILED);

            if (allDone)
            {
                boolean anyFailed = volClones.stream().anyMatch(ci -> ci.getStatus() == CloneInfo.CloneStatus.FAILED);

                notifyCloneStatus(cloneInfo.rscName, cloneInfo.getVlmNr(), anyFailed);

                // remove from activeClones once CLONE_FINISHED is set.
            }
        }
    }

    public static class CloneInfo implements Comparable<CloneInfo>
    {
        private final ResourceName rscName;
        private final String suffix;
        private final VlmProviderObject<Resource> srcVlmData;
        private final VlmProviderObject<Resource> dstVlmData;
        private final DeviceHandler.CloneStrategy cloneStrategy;

        private @Nullable CloneDaemon cloneDaemon;
        private CloneStatus status;

        public enum CloneStatus
        {
            PROGRESS,
            FAILED,
            FINISH
        }

        CloneInfo(
            VlmProviderObject<Resource> srcVlmDataRef,
            VlmProviderObject<Resource> dstVlmDataRef,
            DeviceHandler.CloneStrategy cloneStrategyRef
            )
        {
            rscName = dstVlmDataRef.getRscLayerObject().getResourceName();
            suffix = dstVlmDataRef.getRscLayerObject().getResourceNameSuffix();
            srcVlmData = srcVlmDataRef;
            dstVlmData = dstVlmDataRef;
            cloneStrategy = cloneStrategyRef;
            status = CloneStatus.PROGRESS;
        }

        public VlmProviderObject<Resource> getSrcVlmData()
        {
            return srcVlmData;
        }

        public VlmProviderObject<Resource> getDstVlmData()
        {
            return dstVlmData;
        }

        public DeviceHandler.CloneStrategy getCloneStrategy()
        {
            return cloneStrategy;
        }

        public DeviceProviderKind getKind()
        {
            return dstVlmData.getProviderKind();
        }

        public ResourceName getResourceName()
        {
            return rscName;
        }

        public VolumeNumber getVlmNr()
        {
            return dstVlmData.getVlmNr();
        }

        public String getSuffix()
        {
            return suffix;
        }

        public void setCloneDaemon(CloneDaemon cloneDaemonRef)
        {
            cloneDaemon = cloneDaemonRef;
        }

        public @Nullable CloneDaemon getCloneDaemon()
        {
            return cloneDaemon;
        }

        public void setCloneStatus(boolean success)
        {
            this.status = success ? CloneStatus.FINISH : CloneStatus.FAILED;
        }

        public CloneStatus getStatus()
        {
            return status;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (other == null || getClass() != other.getClass())
            {
                return false;
            }
            CloneInfo cloneInfo = (CloneInfo) other;
            return getVlmNr() == cloneInfo.getVlmNr() && getResourceName().equals(cloneInfo.getResourceName()) &&
                getSuffix().equals(cloneInfo.getSuffix());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(getResourceName(), getSuffix(), getVlmNr());
        }

        @Override
        public int compareTo(CloneInfo other)
        {
            return (getResourceName().getName() + getSuffix() + getVlmNr())
                .compareTo(other.getResourceName().getName() + other.getSuffix() + other.getVlmNr());
        }

        @Override
        public String toString()
        {
            return cloneStrategy.name() + "(" +
                srcVlmData.getRscLayerObject().getResourceName() +
                srcVlmData.getRscLayerObject().getResourceNameSuffix() + "/" + srcVlmData.getVlmNr() + "->" +
                getResourceName() + getSuffix() + "/" + dstVlmData.getVlmNr() + ")";
        }
    }
}
