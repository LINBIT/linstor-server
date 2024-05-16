package com.linbit.linstor.clone;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.lvm.LvmThinData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.StringUtils;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

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

    private boolean serviceStarted = false;

    @Inject
    CloneService(
        ErrorReporter errorReporterRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        VolumeDiskStateEvent volumeDiskStateEventRef,
        ExtCmdFactory extCmdFactoryRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef
    )
    {
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;
        extCmdFactory = extCmdFactoryRef;
        reconfigurationLock = reconfigurationLockRef;

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
    public void shutdown()
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

    private void cleanupDevices(CloneInfo cloneInfo)
    {
        Lock writeLock = reconfigurationLock.writeLock();
        try
        {
            writeLock.lock();
            // TODO cleanup
//            cloneInfo.getDeviceProvider().doCloneCleanup(cloneInfo);
        }
//        catch (StorageException exc)
//        {
//            errorReporter.reportError(exc);
//        }
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
                            LvmThinData<Resource> srcLvmData = (LvmThinData<Resource>)srcVlmData;
                            LvmThinData<Resource> dstLvmData = (LvmThinData<Resource>)dstVlmData;
                            cmd = doLvmThinClone(
                                dstLvmData.getVolumeGroup(), srcLvmData.getIdentifier(), dstLvmData.getIdentifier());
                        }
                        break;
                        case ZFS_CLONE:
                        {
                            ZfsData<Resource> srcZfsData = (ZfsData<Resource>) srcVlmData;
                            ZfsData<Resource> dstZfsData = (ZfsData<Resource>) dstVlmData;
                            cmd = doZFSClone(
                                dstZfsData.getZPool(),
                                srcZfsData.getCloneDevicePath(),
                                dstZfsData.getCloneDevicePath());
                        }
                            break;
                        case ZFS_COPY:
                        {
                            ZfsData<Resource> srcZfsData = (ZfsData<Resource>) srcVlmData;
                            ZfsData<Resource> dstZfsData = (ZfsData<Resource>) dstVlmData;
                            cmd = doZFSCopy(
                                dstZfsData.getZPool(),
                                srcZfsData.getCloneDevicePath(),
                                dstZfsData.getCloneDevicePath());
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
        ZfsCommands.restoreSnapshotFullName(
            extCmdFactory.create(),
            zpoolName,
            srcIdentifier,
            dstIdentifier
        );
        return null;
    }

    private String[] doZFSCopy(String zpoolName, String srcIdentifier, String dstIdentifier)
    {
        return new String[]
            {
                "timeout",
                "0",
                "bash",
                "-c",
                String.format(
                    "trap 'kill -HUP 0' SIGTERM; " +
                        "set -o pipefail; " +
                        "(" +
                        "zfs send --embed --dedup --large-block %s/%s | " +
                        // if send/recv fails no new volume will be there, so destroy isn't needed
                        "zfs receive -F %s/%s && zfs destroy -r %s/%s@%% ;" +
                        ")& wait $!",
                    zpoolName,
                    srcIdentifier, // usually snapshot name
                    zpoolName,
                    dstIdentifier,
                    zpoolName,
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

    private void postClone(
        boolean successRef,
        CloneInfo cloneInfo
    )
    {
        cleanupDevices(cloneInfo);

        synchronized (activeClones)
        {
            cloneInfo.setCloneStatus(successRef); // do not move this out of the synchronized block
            Set<CloneInfo> volClones = volumeClones(cloneInfo);
            boolean allDone = volClones.stream().allMatch(ci -> ci.getStatus() == CloneInfo.CloneStatus.FINISH ||
                ci.getStatus() == CloneInfo.CloneStatus.FAILED);

            if (allDone)
            {
                boolean anyFailed = volClones.stream().anyMatch(ci -> ci.getStatus() == CloneInfo.CloneStatus.FAILED);

                volumeDiskStateEvent.get().triggerEvent(
                    ObjectIdentifier.volumeDefinition(cloneInfo.getResourceName(), cloneInfo.getVlmNr()),
                    anyFailed ? "CloningFailed" : "CloningDone");

                controllerPeerConnector.getControllerPeer().sendMessage(
                    ctrlStltSerializer.onewayBuilder(InternalApiConsts.API_NOTIFY_CLONE_UPDATE)
                        .notifyCloneUpdate(
                            cloneInfo.getResourceName().getDisplayName(),
                            cloneInfo.getVlmNr().value,
                            !anyFailed)
                        .build(),
                    InternalApiConsts.API_NOTIFY_CLONE_UPDATE
                );

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

        private CloneDaemon cloneDaemon;
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

        public CloneDaemon getCloneDaemon()
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
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            CloneInfo cloneInfo = (CloneInfo) o;
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
            return getKind().name() + "(" +
                srcVlmData.getRscLayerObject().getResourceName() +
                srcVlmData.getRscLayerObject().getResourceNameSuffix() + "/" + srcVlmData.getVlmNr() + "->" +
                getResourceName() + getSuffix() + "/" + dstVlmData.getVlmNr() + ")";
        }
    }
}
