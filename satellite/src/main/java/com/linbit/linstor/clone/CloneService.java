package com.linbit.linstor.clone;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.layer.ReadOnlySnapshotHandle;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils;
import com.linbit.linstor.logging.ErrorReporter;
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

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

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
    private final StltConfigAccessor stltConfigAccessor;

    private final ServiceName instanceName;
    private final ConcurrentSkipListSet<CloneInfo> activeClones;
    private final ThreadGroup threadGroup;
    private final ReadWriteLock reconfigurationLock;
    private final Provider<DeviceHandler> resourceProcessorProvider;

    private boolean serviceStarted = false;

    @Inject
    CloneService(
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        VolumeDiskStateEvent volumeDiskStateEventRef,
        ExtCmdFactory extCmdFactoryRef,
        StltConfigAccessor stltConfigAccessorRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        Provider<DeviceHandler> resourceProcessorRef
    )
    {
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;
        extCmdFactory = extCmdFactoryRef;
        stltConfigAccessor = stltConfigAccessorRef;
        reconfigurationLock = reconfigurationLockRef;
        resourceProcessorProvider = resourceProcessorRef;

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
        cleanupActiveClones();
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

    private void cleanupActiveClones()
    {
        for (CloneInfo cloneInfo : activeClones)
        {
            if (cloneInfo.getCloneDaemon() != null)
            {
                cloneInfo.getCloneDaemon().shutdown();
            }
            cleanupDevices(cloneInfo);
        }
        activeClones.clear();
    }

    public void clear()
    {
        cleanupActiveClones();
    }

    private void doLVMCloneSourceCleanup(LvmData<Resource> srcData, String cloneName) throws StorageException
    {
        final String srcFullSnapshotName = AbsStorageProvider.getLVMCloneSnapshotNameFull(
            srcData.getIdentifier(), cloneName, srcData.getVlmNr().value
        );

        final @Nullable String vlmGroup = srcData.getVolumeGroup();
        if (vlmGroup != null)
        {
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

    public void cleanupSrcCloneSnapshots(
        VlmProviderObject<Resource> srcVlmData, ResourceName rscName, boolean isZfsCloneStrategy)
        throws StorageException
    {
        AbsRscLayerObject<Resource> srcRscData = srcVlmData.getRscLayerObject();
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
                    doLVMCloneSourceCleanup((LvmData<Resource>) vlmObj, rscName.displayValue);
                    break;
                case ZFS:
                case ZFS_THIN:
                    doZFSCloneSourceCleanup((ZfsData<Resource>) vlmObj, rscName.displayValue, isZfsCloneStrategy);
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

    private void cleanupDevices(CloneInfo cloneInfo)
    {
        Lock writeLock = reconfigurationLock.writeLock();
        writeLock.lock();
        try
        {
            VlmProviderObject<Resource> srcVlmData = cloneInfo.srcVlmData;
            VlmProviderObject<Resource> tgtVlmData = cloneInfo.dstVlmData;
            AbsRscLayerObject<Resource> srcRscData = srcVlmData.getRscLayerObject();
            AbsRscLayerObject<Resource> tgtRscData = tgtVlmData.getRscLayerObject();

            try
            {
                resourceProcessorProvider.get().processAfterClone(
                    srcVlmData,
                    // use root vlm data to traverse all rsc layers
                    tgtVlmData.getRscLayerObject()
                        .getAbsResource()
                        .getLayerData()
                        .getVlmProviderObject(tgtVlmData.getVlmNr()),
                    tgtVlmData.getCloneDevicePath()
                );
            }
            catch (StorageException | RuntimeException exc)
            {
                // the clone is over either way, so the devices opened for it have to be closed nevertheless
                try
                {
                    closeCloneDevices(cloneInfo);
                }
                catch (StorageException | RuntimeException closeExc)
                {
                    exc.addSuppressed(closeExc);
                }
                throw exc;
            }
            closeCloneDevices(cloneInfo);

            // reset passThroughMode
            // "clone passthrough mode" is based on targetRscData for both, srcRscData and targetRscData
            srcRscData.removeClonePassthroughMode(tgtRscData);
            tgtRscData.removeClonePassthroughMode(tgtRscData);

            // cleanup all clone snapshots
            cleanupSrcCloneSnapshots(
                srcVlmData,
                cloneInfo.getResourceName(),
                cloneInfo.cloneStrategy == DeviceHandler.CloneStrategy.ZFS_CLONE
            );

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
        catch (StorageException exc)
        {
            errorReporter.reportError(exc);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Closes the source snapshot and the target that were opened for the clone.
     */
    private void closeCloneDevices(CloneInfo cloneInfo) throws StorageException
    {
        if (cloneInfo.srcSnapshotHandle != null)
        {
            cloneInfo.srcSnapshotHandle.close();
        }
        resourceProcessorProvider.get().closeCloneTarget(cloneInfo.dstVlmData);
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

    /**
     * Starts cloning the given source volume into the given target volume in the background, unless a clone of the
     * target volume is already running.
     *
     * @param srcVlmData the volume data of the clone source on the layer the clone is done on
     * @param dstVlmData the volume data of the clone target on the same layer
     * @param strat how to clone. The volume data has to match it, e.g. {@link DeviceHandler.CloneStrategy#DD} copies
     *     between their {@link VlmProviderObject#getCloneDevicePath()}s, while the ZFS and LVM strategies expect
     *     storage layer data of the respective provider.
     * @param srcSnapshotHandle the clone snapshot of the source, opened with
     *     {@link DeviceHandler#openSnapshotReadOnly} and a {@link com.linbit.linstor.layer.ReadOnlyOpener.ForClone}.
     *     Required if the strategy {@link DeviceHandler.CloneStrategy#needsOpenDevices() needs open devices},
     *     {@code null} otherwise. This service takes over closing it: when the clone is done, or right away if a clone
     *     of the target volume is already running. If this method throws, the caller has to close it (closing it
     *     again is harmless).
     */
    @SuppressFBWarnings(
        {
            "JLM_JSR166_UTILCONCURRENT_MONITORENTER", "BC_UNCONFIRMED_CAST"
        }
    )
    public void startClone(
        VlmProviderObject<Resource> srcVlmData,
        VlmProviderObject<Resource> dstVlmData,
        DeviceHandler.CloneStrategy strat,
        @Nullable ReadOnlySnapshotHandle srcSnapshotHandle
    ) throws StorageException
    {
        if (strat.needsOpenDevices() != (srcSnapshotHandle != null))
        {
            throw new ImplementationError(
                "Clone strategy " + strat + (strat.needsOpenDevices() ? " requires" : " does not use") +
                    " an opened source snapshot, but " + (srcSnapshotHandle == null ? "none" : "one") + " was given"
            );
        }
        final CloneInfo cloneInfo = new CloneInfo(
            srcVlmData,
            dstVlmData,
            strat,
            srcSnapshotHandle
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
                            /*
                             * Clone from the clone snapshot that was taken while the source was suspended, not from
                             * the source volume itself: the target is only created after the source was resumed, so
                             * a snapshot of the source volume would contain later writes, and each volume of the
                             * resource would be taken at a different point in time.
                             */
                            final String srcFullSnapshotName = AbsStorageProvider.getLVMCloneSnapshotNameFull(
                                srcLvmData.getIdentifier(),
                                cloneInfo.getResourceName().displayValue,
                                srcLvmData.getVlmNr().value
                            );
                            cmd = doLvmThinClone(
                                dstLvmData.getVolumeGroup(), srcFullSnapshotName, dstLvmData.getIdentifier());
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
                            String ddBlocksize = "64k";
                            ResourceDefinition dstRscDfn = dstVlmData.getRscLayerObject()
                                .getAbsResource().getResourceDefinition();
                            ResourceDefinition srcRscDfn = srcVlmData.getRscLayerObject()
                                .getAbsResource().getResourceDefinition();
                            PriorityProps prioProps = new PriorityProps();
                            prioProps.addProps(dstRscDfn.getProps());
                            prioProps.addProps(dstRscDfn.getResourceGroup().getProps());
                            prioProps.addProps(srcRscDfn.getProps());
                            prioProps.addProps(srcRscDfn.getResourceGroup().getProps());
                            if (dstVlmData.getStorPool() != null)
                            {
                                prioProps.addProps(dstVlmData.getStorPool().getProps());
                            }
                            if (srcVlmData.getStorPool() != null)
                            {
                                prioProps.addProps(srcVlmData.getStorPool().getProps());
                            }
                            prioProps.addProps(stltConfigAccessor.getReadonlyProps());
                            @Nullable String bsProp = prioProps.getProp(
                                ApiConsts.KEY_CLONE_DD_BLOCKSIZE,
                                ApiConsts.NAMESPC_CLONE);
                            if (bsProp != null && !bsProp.isEmpty())
                            {
                                ddBlocksize = bsProp;
                            }

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
                                    "bs=" + ddBlocksize,
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
                else if (srcSnapshotHandle != null)
                {
                    // the already running clone has its own handle
                    srcSnapshotHandle.close();
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
                    dstIdentifier)
            };
    }

    /**
     * Returns all clones belonging to the same resource/volume
     *
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
        /** The opened clone snapshot of the source, if the clone strategy needs open devices */
        private final @Nullable ReadOnlySnapshotHandle srcSnapshotHandle;

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
            DeviceHandler.CloneStrategy cloneStrategyRef,
            @Nullable ReadOnlySnapshotHandle srcSnapshotHandleRef
            )
        {
            rscName = dstVlmDataRef.getRscLayerObject().getResourceName();
            suffix = dstVlmDataRef.getRscLayerObject().getResourceNameSuffix();
            srcVlmData = srcVlmDataRef;
            dstVlmData = dstVlmDataRef;
            cloneStrategy = cloneStrategyRef;
            srcSnapshotHandle = srcSnapshotHandleRef;
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
            if (!(other instanceof CloneInfo cloneInfo))
            {
                return false;
            }
            return getVlmNr().equals(cloneInfo.getVlmNr()) && getResourceName().equals(cloneInfo.getResourceName()) &&
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
