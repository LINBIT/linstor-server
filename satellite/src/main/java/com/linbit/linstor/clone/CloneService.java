package com.linbit.linstor.clone;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

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
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef
    )
    {
        errorReporter = errorReporterRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;
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
            cloneInfo.getDeviceProvider().doCloneCleanup(cloneInfo);
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

    public boolean isRunning(@Nonnull ResourceName rscName, @Nonnull VolumeNumber vlmNr, @Nonnull String suffix)
    {
        return activeClones.stream().anyMatch(cloneInfo ->
            cloneInfo.getResourceName().equals(rscName) && cloneInfo.getVlmNr().equals(vlmNr) &&
                cloneInfo.getSuffix().equals(suffix));
    }

    public <VLM_DATA extends AbsStorageVlmData<Resource>> void startClone(
        @Nonnull VLM_DATA srcVlmData,
        @Nonnull VLM_DATA dstVlmData,
        AbsStorageProvider<?, ?, ?> provider
    ) throws StorageException
    {
        final CloneInfo cloneInfo = new CloneInfo(
            srcVlmData,
            dstVlmData,
            provider
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

                    String[] cmd = cloneInfo.getCloneCommand();

                    if (cmd == null || cmd.length == 0)
                    {
                        throw new StorageException("Clone not supported for DeviceProviderKind: " + cloneInfo.getKind());
                    }
                    activeClones.add(cloneInfo);

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
     * Returns all clones belonging to the same resource/volume
     *
     * @param ci
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

                volClones.forEach(activeClones::remove);
            }
        }
    }

    public static class CloneInfo implements Comparable<CloneInfo>
    {
        private final ResourceName rscName;
        private final String suffix;
        private final AbsStorageVlmData<Resource> srcVlmData;
        private final AbsStorageVlmData<Resource> dstVlmData;
        private final AbsStorageProvider<?, ?, ?> deviceProvider;

        private CloneDaemon cloneDaemon;
        private CloneStatus status;

        public enum CloneStatus
        {
            PROGRESS,
            FAILED,
            FINISH
        }

        CloneInfo(
            @Nonnull AbsStorageVlmData<Resource> srcVlmDataRef,
            @Nonnull AbsStorageVlmData<Resource> dstVlmDataRef,
            @Nonnull AbsStorageProvider<?, ?, ?> providerRef
        )
        {
            rscName = dstVlmDataRef.getRscLayerObject().getResourceName();
            suffix = dstVlmDataRef.getRscLayerObject().getResourceNameSuffix();
            srcVlmData = srcVlmDataRef;
            dstVlmData = dstVlmDataRef;
            deviceProvider = providerRef;
            status = CloneStatus.PROGRESS;
        }

        public AbsStorageVlmData<Resource> getSrcVlmData()
        {
            return srcVlmData;
        }

        public AbsStorageVlmData<Resource> getDstVlmData()
        {
            return dstVlmData;
        }

        public AbsStorageProvider<?, ?, ?> getDeviceProvider()
        {
            return deviceProvider;
        }

        public String[] getCloneCommand()
        {
            return getDeviceProvider().getCloneCommand(this);
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
