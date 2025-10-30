package com.linbit.linstor.core.ebs;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.ebs.EbsData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.tasks.EbsStatusPollTask;
import com.linbit.linstor.tasks.TaskScheduleService;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;

import com.amazonaws.AbortedException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest;
import com.amazonaws.services.ec2.model.DescribeSnapshotsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesModificationsRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesModificationsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;
import com.amazonaws.services.ec2.model.Volume;
import com.amazonaws.services.ec2.model.VolumeModification;

@Singleton
public class EbsStatusManagerService implements SystemService
{
    public static final long DFLT_POlL_WAIT = 5_000;

    private static final long DFLT_POLL_TIMEOUT_MS = 60_000;
    // private static final long DFLT_POLL_TIMEOUT_MS = 10_000;
    private static final int MAX_ENTRIES_PER_PAGE = 1000;

    public static final ServiceName SERVICE_NAME;
    public static final String SERVICE_INFO = "EbsStatusPoll";


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

    private final ServiceName instanceName;

    private boolean initialized = false;
    private volatile boolean keepRunning = true;
    private @Nullable Thread thread;

    private final Object syncQueueAndThread = new Object();
    private final ArrayBlockingQueue<PollStatus> queue = new ArrayBlockingQueue<>(2);
    private final PollConfig defaultCfg = new PollConfig();

    private final Map<EbsRemote, EbsRemoteIds> idsByRemote = new HashMap<>();

    private final ErrorReporter errorReporter;
    private final AccessContext sysCtx;
    private final CtrlSecurityObjects secObjs;
    private final ResourceDefinitionMap rscDfnMap;
    private final RemoteMap remoteMap;
    private final SystemConfRepository sysCfgRepo;
    private final TaskScheduleService taskScheduleService;
    private final LockGuardFactory lockGuardFactory;

    private final Set<Resource> knownRscs;
    private final Set<Snapshot> knownSnaps;

    @Inject
    public EbsStatusManagerService(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlSecurityObjects secObjsRef,
        ResourceDefinitionMap rscDfnMapRef,
        RemoteMap remoteMapRef,
        SystemConfRepository sysCfgRepoRef,
        TaskScheduleService taskScheduleServiceRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        secObjs = secObjsRef;
        rscDfnMap = rscDfnMapRef;
        remoteMap = remoteMapRef;
        sysCfgRepo = sysCfgRepoRef;
        taskScheduleService = taskScheduleServiceRef;
        lockGuardFactory = lockGuardFactoryRef;

        knownRscs = new HashSet<>();
        knownSnaps = new HashSet<>();

        instanceName = SERVICE_NAME;
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        synchronized (syncQueueAndThread)
        {
            if (!initialized)
        {
                initialized = true;
                initialize();
            }
            if (thread == null)
            {
                keepRunning = true;
                thread = new Thread(this::run, instanceName.getDisplayName());
                thread.start();
            }
        }
    }

    @Override
    public void shutdown(boolean ignoredJvmShutdownRef)
    {
        synchronized (syncQueueAndThread)
        {
            keepRunning = false;
            if (thread != null)
            {
                thread.interrupt();
            }
        }
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        Thread joinThr = null;
        synchronized (syncQueueAndThread)
        {
            joinThr = thread;
        }
        if (joinThr != null)
        {
            joinThr.join(timeoutRef);
        }
    }

    public void initialize()
    {
        try
        {
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                Iterator<Resource> rscIt = rscDfn.iterateResource(sysCtx);
                while (rscIt.hasNext())
                {
                    addIfEbs(rscIt.next());
                }

                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(sysCtx))
                {
                    for (Snapshot snap : snapDfn.getAllSnapshots(sysCtx))
                    {
                        addIfEbs(snap);
                    }
                }
            }

            taskScheduleService.addTask(new EbsStatusPollTask(this, DFLT_POLL_TIMEOUT_MS));
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void addIfEbs(Resource rscRef) throws AccessDeniedException
    {
        genericAddIfEbs(rscRef, knownRscs, this::addVolume);
    }

    public void addIfEbs(Snapshot snapRef) throws AccessDeniedException
    {
        genericAddIfEbs(snapRef, knownSnaps, this::addSnap);
    }

    private <RSC extends AbsResource<RSC>> void genericAddIfEbs(
        RSC rscOrSnapRef,
        Set<RSC> knownRscOrSnapRef,
        Consumer<EbsData<RSC>> consumerRef
    )
        throws AccessDeniedException
    {
        if (EbsUtils.isEbs(sysCtx, rscOrSnapRef))
        {
            synchronized (knownRscOrSnapRef)
            {
                if (!knownRscOrSnapRef.contains(rscOrSnapRef))
                {
                    addAllEbsData(rscOrSnapRef, consumerRef);
                    knownRscOrSnapRef.add(rscOrSnapRef);
                }
            }
        }
    }

    private <RSC extends AbsResource<RSC>> void addAllEbsData(RSC absRscRef, Consumer<EbsData<RSC>> addFunctionRef)
        throws AccessDeniedException
    {
        Set<AbsRscLayerObject<RSC>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            absRscRef.getLayerData(sysCtx),
            DeviceLayerKind.STORAGE
        );
        for (AbsRscLayerObject<RSC> storRscData : storRscDataSet)
        {
            for (VlmProviderObject<RSC> vlmData : storRscData.getVlmLayerObjects().values())
            {
                if (vlmData instanceof EbsData)
                {
                    addFunctionRef.accept((EbsData<RSC>) vlmData);
                }
            }
        }
    }

    public void pollAsync()
    {
        offer(defaultCfg);
    }

    private PollStatus offer(final PollConfig pollCfg)
    {
        PollStatus ret;
        synchronized (syncQueueAndThread)
        {
            // "found" looks unnecessary right now, but the plan is that PollConfig might get more complex in the future
            // and then we might want to implement something like "is a same pollConfig already in the queue? if so, we
            // just want to add ourself to its notify-list
            boolean found = !queue.isEmpty();
            if (found)
            {
                ret = queue.peek();
            }
            else
            {
                ret = new PollStatus(pollCfg);
                queue.add(ret);
            }
        }
        return ret;
    }

    public boolean pollAndWait(long timeoutInMs) throws InterruptedException
    {
        final PollStatus pollStatus;
        synchronized (syncQueueAndThread)
        {
            pollStatus = offer(new PollConfig());

            long start = System.currentTimeMillis();
            long remainingTimeout = timeoutInMs;
            while (keepRunning && !pollStatus.answerReceived && remainingTimeout > 0)
            {
                synchronized (pollStatus)
                {
                    pollStatus.wait(remainingTimeout);
                }
                if (!pollStatus.answerReceived)
                {
                    remainingTimeout = Math.max(0, timeoutInMs - (System.currentTimeMillis() - start));
                }
            }
        }
        return pollStatus.answerReceived;
    }

    private void run()
    {
        while (keepRunning)
        {
            PollStatus pollStatus = null;
            try
            {
                pollStatus = queue.take();
            }
            catch (InterruptedException exc)
            {
                // ignored
                Thread.currentThread().interrupt(); // re-interrupt to keep / restore the interrupted flag
            }
            if (pollStatus != null)
            {
                try
                {
                    pollEbsStatus(pollStatus.pollCfg);

                    synchronized (pollStatus)
                    {
                        pollStatus.answerReceived = true;
                        pollStatus.notifyAll();
                    }
                }
                catch (AbortedException exc)
                {
                    if (keepRunning) // otherwise, ignore exception
                    {
                        errorReporter.reportError(exc);
                    }
                }
            }
        }
        synchronized (this)
        {
            if (thread == Thread.currentThread())
            {
                thread = null;
            }
        }
    }

    public void addVolume(EbsData<Resource> vlmData)
    {
        synchronized (idsByRemote)
        {
            try
            {
                lazyGet(getEbsRemote(vlmData)).allVlmIds.put(EbsUtils.getEbsVlmId(sysCtx, vlmData), vlmData);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private EbsRemote getEbsRemote(EbsData<?> vlmDataRef)
    {
        try
        {
            return EbsUtils.getEbsRemote(
                sysCtx,
                remoteMap,
                vlmDataRef.getStorPool(),
                sysCfgRepo.getStltConfForView(sysCtx)
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void addSnap(EbsData<Snapshot> snapData)
    {
        synchronized (idsByRemote)
        {
            try
            {
                lazyGet(getEbsRemote(snapData)).allSnapIds.put(EbsUtils.getEbsSnapId(sysCtx, snapData), snapData);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private void pollEbsStatus(PollConfig pollConfigRef)
    {
        // check if we have master passphrase
        if (secObjs.areAllSet())
        {
            Map<EbsRemote, EbsRemoteIds> localIdsByRemote;
            synchronized (idsByRemote)
            {
                // shallow copy, the inner Sets are still references that can still be updated from a different
                // thread!
                localIdsByRemote = new HashMap<>(idsByRemote);
            }
            for (Map.Entry<EbsRemote, EbsRemoteIds> entry : localIdsByRemote.entrySet())
            {
                EbsRemote remote = entry.getKey();
                EbsRemoteIds ids = entry.getValue();

                try
                {
                    AmazonEC2 client = getClient(remote);
                    if (client != null)
                    {
                        updateVolumes(client, ids.allVlmIds);
                        updateSnapshots(client, ids.allSnapIds);
                    }
                }
                catch (AccessDeniedException | DatabaseException exc)
                {
                    errorReporter.reportError(new ImplementationError(exc));
                    errorReporter.logError("EbsStatusManager shutting down.");
                    keepRunning = false;
                }
            }
        }
    }

    private void updateVolumes(AmazonEC2 client, Map<String, EbsData<Resource>> vlmsMapRef) throws AccessDeniedException
    {
        Map<String, EbsData<Resource>> vlmsMapCopy = new HashMap<>(vlmsMapRef);

        /*
         * https://docs.amazonaws.cn/en_us/AWSEC2/latest/UserGuide/ebs-describing-volumes.html
         *
         * describeVolumes.state:
         *  "in-use" / "available" / "creating" / "deleting" / "deleted" / "error"
         * describeVolumeModifications.modificationState(if exists):
         *  "" / "optimizing"
         * describeVolumeModifications.progress (if exists):
         *  "0" / ... / "99"
         */

        final ArrayList<String> vlmIdList = new ArrayList<>(vlmsMapCopy.keySet());
        final int vlmIdListSize = vlmIdList.size();
        final int vlmIdListPages = vlmIdListSize / MAX_ENTRIES_PER_PAGE;
        for (int page = 0; page <= vlmIdListPages; page++)
        {
            List<String> currentVlmIdList = vlmIdList.subList(
                page * MAX_ENTRIES_PER_PAGE,
                Math.min((page + 1) * MAX_ENTRIES_PER_PAGE, vlmIdListSize)
            );

            DescribeVolumesResult describeVolumesResult = client.describeVolumes(
                new DescribeVolumesRequest().withVolumeIds(currentVlmIdList)
            );

            DescribeVolumesModificationsResult describeVlmMods = client.describeVolumesModifications(
                new DescribeVolumesModificationsRequest()
            );
            Map<String, VolumeModification> vlmModByEbsId = new HashMap<>();
            for (VolumeModification vlmMod : describeVlmMods.getVolumesModifications())
            {
                vlmModByEbsId.put(vlmMod.getVolumeId(), vlmMod);
            }

            for (Volume amaVlm : describeVolumesResult.getVolumes())
            {
                // remove from map so we can easily track which local vlmData were deleted in the meantime
                // so we can get rid of them after this loop
                EbsData<Resource> vlmData = vlmsMapCopy.remove(amaVlm.getVolumeId());

                // vlmData might be null... might be a linstor-external EBS. noop
                if (vlmData != null)
                {
                    VolumeModification vlmMod = vlmModByEbsId.get(amaVlm.getVolumeId());
                    Resource rsc = vlmData.getVolume().getAbsResource();
                    if (!rsc.isDeleted())
                    {
                        Peer peer = rsc.getNode().getPeer(sysCtx);
                        if (peer != null)
                        {
                            ReadWriteLock satelliteStateLock = peer.getSatelliteStateLock();
                            satelliteStateLock.writeLock().lock();
                            try
                            {
                                SatelliteState rscStates = peer.getSatelliteState();
                                ResourceName rscName = rsc.getResourceDefinition().getName();
                                rscStates.setOnResource(
                                    rscName,
                                    SatelliteResourceState::setInUse,
                                    EbsUtils.EBS_VLM_STATE_IN_USE.equalsIgnoreCase(amaVlm.getState())
                                );
                                String diskState = amaVlm.getState();
                                if (vlmMod != null)
                                {
                                    String modState = vlmMod.getModificationState();
                                    if (!modState.isEmpty() && !EbsUtils.EBS_VLM_STATE_COMPLETED.equals(modState))
                                    {
                                        diskState += ", " +
                                            vlmMod.getModificationState() + ": " +
                                            vlmMod.getProgress() +
                                            "%";
                                    }
                                }

                                rscStates.setOnVolume(
                                    rscName,
                                    vlmData.getVlmNr(),
                                    SatelliteVolumeState::setDiskState,
                                    diskState
                                );
                            }
                            finally
                            {
                                satelliteStateLock.writeLock().unlock();
                            }
                        }
                    }
                }
            }
        }

        synchronized (idsByRemote)
        {
            for (String key : vlmsMapCopy.keySet())
            {
                EbsData<Resource> vlmData = vlmsMapRef.get(key);
                if (vlmData.getVolume().isDeleted())
                {
                    vlmsMapRef.remove(key);
                }
                Resource rsc = vlmData.getRscLayerObject().getAbsResource();
                if (rsc.isDeleted())
                {
                    knownRscs.remove(rsc);
                }
            }
        }
    }

    private void updateSnapshots(AmazonEC2 client, Map<String, EbsData<Snapshot>> snapMapRef)
        throws AccessDeniedException, DatabaseException
    {
        Map<String, EbsData<Snapshot>> snapMapCopy = new HashMap<>(snapMapRef);
        /*
         * https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-snapshots.html
         *
         * describeSnap.state (Strings from com.amazonaws.services.ec2.model.SnapshotState):
         * "pending" / "completed" / "recoverable" / "recovering" / "error"
         * describeSnap.progress:
         * "0%" / ... / "99%"
         */

        ArrayList<String> snapIdList = new ArrayList<>(snapMapCopy.keySet());
        final int snapIdListSize = snapIdList.size();
        final int snapIdListPages = snapIdListSize / MAX_ENTRIES_PER_PAGE;
        for (int page = 0; page <= snapIdListPages; page++)
        {
            List<String> currentSnapIdList = snapIdList.subList(
                page * MAX_ENTRIES_PER_PAGE,
                Math.min((page + 1) * MAX_ENTRIES_PER_PAGE, snapIdListSize)
            );

            DescribeSnapshotsResult describeSnapResult = client.describeSnapshots(
                new DescribeSnapshotsRequest().withSnapshotIds(currentSnapIdList)
            );

            for (com.amazonaws.services.ec2.model.Snapshot amaSnap : describeSnapResult.getSnapshots())
            {
                // remove from map so we can easily track which local vlmData were deleted in the meantime
                // so we can get rid of them after this loop
                EbsData<Snapshot> snapVlmData = snapMapCopy.remove(amaSnap.getSnapshotId());
                // snapVlmData might be null... might be a linstor-external EBS snapshot. noop
                if (snapVlmData != null)
                {
                    SnapshotVolume snapVlm = (SnapshotVolume) snapVlmData.getVolume();
                    if (!snapVlm.isDeleted())
                    {
                        String diskState = amaSnap.getState();
                        if (!EbsUtils.EBS_SNAP_STATE_COMPLETED.equalsIgnoreCase(diskState))
                        {
                            // "%" is already included from .getProgress()
                            diskState += ": " + amaSnap.getProgress();
                        }
                        snapVlm.setState(sysCtx, diskState);
                    }
                }
            }
        }


        synchronized (idsByRemote)
        {
            for (String key : snapMapCopy.keySet())
            {
                EbsData<Snapshot> snapVlmData = snapMapRef.get(key);
                if (snapVlmData.getVolume().isDeleted())
                {
                    snapMapRef.remove(key);
                }
            }
        }
    }

    private @Nullable AmazonEC2 getClient(EbsRemote remoteRef) throws AccessDeniedException
    {
        AmazonEC2 client;
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.REMOTE_MAP))
        {
            client = AmazonEC2ClientBuilder.standard()
                .withEndpointConfiguration(
                    new EndpointConfiguration(
                        remoteRef.getUrl(sysCtx).toString(),
                        remoteRef.getRegion(sysCtx)
                    )
                )
                .withCredentials(
                    new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                            remoteRef.getDecryptedAccessKey(sysCtx),
                            remoteRef.getDecryptedSecretKey(sysCtx)
                        )
                    )
                )
                .build();
        }
        return client;
    }

    private EbsRemoteIds lazyGet(EbsRemote ebsRemote)
    {
        EbsRemoteIds ret = idsByRemote.get(ebsRemote);
        if (ret == null)
        {
            ret = new EbsRemoteIds(ebsRemote);
            idsByRemote.put(ebsRemote, ret);
        }
        return ret;
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
        return keepRunning;
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {
    }

    private static class PollConfig
    {
    }

    private static class PollStatus
    {
        boolean answerReceived = false;
        PollConfig pollCfg;

        PollStatus(PollConfig pollCfgRef)
        {
            pollCfg = pollCfgRef;
        }
    }

    private static class EbsRemoteIds
    {
        @SuppressWarnings("unused")
        private final EbsRemote remote;

        private final Map<String, EbsData<Resource>> allVlmIds;
        private final Map<String, EbsData<Snapshot>> allSnapIds;

        EbsRemoteIds(EbsRemote remoteRef)
        {
            remote = remoteRef;
            allVlmIds = new HashMap<>();
            allSnapIds = new HashMap<>();
        }
    }
}
