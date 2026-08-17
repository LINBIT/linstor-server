package com.linbit.linstor.core.ebs;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.SystemServiceStopException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;

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
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.VolumeModification;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Singleton
public class EbsStatusManagerService implements SystemService
{
    public static final long DFLT_POLL_WAIT = 5_000;

    private static final long DFLT_POLL_TASK_TIMEOUT_MS = 60_000;
    private static final int MAX_ENTRIES_PER_PAGE = 100;

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
    private volatile boolean keepRunning = false;
    private @Nullable Thread thread;

    private final Object syncQueueAndThread = new Object();
    /**
     * Guarded by syncQueueAndThread. At most one poll request is pending;
     * further requests merge into it until the poll thread claims it.
     */
    private @Nullable PollStatus pendingPollStatus = null;

    private final ErrorReporter errorReporter;
    private final CtrlSecurityObjects secObjs;
    private final ResourceDefinitionMap rscDfnMap;
    private final RemoteMap remoteMap;
    private final SystemConfRepository sysCfgRepo;
    private final TaskScheduleService taskScheduleService;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    public EbsStatusManagerService(
        ErrorReporter errorReporterRef,
        CtrlSecurityObjects secObjsRef,
        ResourceDefinitionMap rscDfnMapRef,
        RemoteMap remoteMapRef,
        SystemConfRepository sysCfgRepoRef,
        TaskScheduleService taskScheduleServiceRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        secObjs = secObjsRef;
        rscDfnMap = rscDfnMapRef;
        remoteMap = remoteMapRef;
        sysCfgRepo = sysCfgRepoRef;
        taskScheduleService = taskScheduleServiceRef;
        lockGuardFactory = lockGuardFactoryRef;

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
        taskScheduleService.addTask(new EbsStatusPollTask(this, DFLT_POLL_TASK_TIMEOUT_MS));
    }

    private <RSC extends AbsResource<RSC>> void addAllEbsData(RSC absRscRef, Consumer<EbsData<RSC>> addFunctionRef)
    {
        Set<AbsRscLayerObject<RSC>> storRscDataSet = LayerRscUtils.getRscDataByLayer(
            absRscRef.getLayerData(),
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
        offer(null, null);
    }

    private PollStatus offer(
        @Nullable Collection<ResourceName> rscDfnsToPollRef,
        @Nullable Collection<SnapshotDefinition.Key> snapDfnKeysToPollRef
    )
    {
        PollStatus ret;
        synchronized (syncQueueAndThread)
        {
            if (!keepRunning)
            {
                // service is stopped or stopping, instant-fail instead of letting the caller wait
                // for whatever timeout they configured
                ret = new PollStatus(null, null);
                ret.future.completeExceptionally(new SystemServiceStopException("EbsStatusManager is not running"));
            }
            else
            {
                if (pendingPollStatus != null)
                {
                    ret = pendingPollStatus;
                    ret.merge(rscDfnsToPollRef, snapDfnKeysToPollRef);
                }
                else
                {
                    ret = new PollStatus(rscDfnsToPollRef, snapDfnKeysToPollRef);
                    pendingPollStatus = ret;
                    syncQueueAndThread.notifyAll();
                }
            }
        }
        return ret;
    }

    public Flux<ApiCallRc> pollFlux(
        long timeoutInMs,
        @Nullable Collection<ResourceName> rscNamesToPollRef,
        @Nullable Collection<SnapshotDefinition.Key> snapDfnsToPollRef
    )
    {
        return pollMono(timeoutInMs, rscNamesToPollRef, snapDfnsToPollRef)
            .thenMany(Flux.empty());
    }

    public Mono<Boolean> pollMono(
        long timeoutInMs,
        @Nullable Collection<ResourceName> rscDfnsToPollRef,
        @Nullable Collection<SnapshotDefinition.Key> snapDfnKeysToPollRef
    )
    {
        // instead of Mono.defer(...).thenReturn, we use this future.thenApply method because the Mono variant would
        // lead to the unwanted situation that if one subscribe-waiter is canceled or times out, all other waiters are
        // also canceled. using the future.thenApply decouples the waiters from each other.
        return Mono.defer(
            () -> Mono.fromFuture(
                offer(rscDfnsToPollRef, snapDfnKeysToPollRef).future.thenApply(ignored -> Boolean.TRUE)
            )
        )
            .timeout(Duration.ofMillis(timeoutInMs))
            .onErrorReturn(Boolean.FALSE);
    }

    /**
     * <p>The caller of this method <b>MUST NOT</b> hold {@link LockObj#RSC_DFN_MAP} or {@link LockObj#REMOTE_MAP}.
     * Otherwise the caller will block the polling thread so the caller is guaranteed to run into the configured
     * timeout.</p>
     *
     * <p>Blocks until either the requested resources/snapshots are updated or the timeout is reached.</p>
     */
    public boolean pollAndWait(
        long timeoutInMs,
        @Nullable Collection<ResourceName> rscDfnsToPollRef,
        @Nullable Collection<SnapshotDefinition.Key> snapDfnKeysToPollRef
    )
        throws InterruptedException
    {
        errorReporter.logTrace("Starting poll and wait for EBS updates. Waiting for max %dms", timeoutInMs);
        boolean success;
        try
        {
            offer(rscDfnsToPollRef, snapDfnKeysToPollRef).future
                .get(timeoutInMs, TimeUnit.MILLISECONDS);
            success = true;
        }
        catch (InterruptedException | ExecutionException | TimeoutException exc)
        {
            errorReporter.reportError(exc);
            success = false;
        }
        errorReporter.logTrace("EBS update done. Received response: %b", success);
        return success;
    }

    private void run()
    {
        while (keepRunning)
        {
            @Nullable PollStatus pollStatus = null;
            @Nullable Collection<ResourceName> rscsToPoll = null;
            @Nullable Collection<SnapshotDefinition.Key> snapDfnsToPoll = null;

            try
            {
                synchronized (syncQueueAndThread)
                {
                    while (pendingPollStatus == null && keepRunning)
                    {
                        syncQueueAndThread.wait();
                    }

                    pollStatus = pendingPollStatus;
                    // claim it. later offer() calls will create a new PollStatus and manage/merge further requests
                    // into it while we are busy processing the current pollStatus
                    pendingPollStatus = null;
                    if (pollStatus != null)
                    {
                        // just to be sure, make a copy of the collections. This is not strictly needed but rather an
                        // additional defensive step
                        rscsToPoll = pollStatus.copyRscsToPoll();
                        snapDfnsToPoll = pollStatus.copySnapDfnsToPoll();
                    }
                }
            }
            catch (InterruptedException exc)
            {
                Thread.currentThread().interrupt(); // re-interrupt to keep / restore the interrupted flag
            }
            if (pollStatus != null)
            {
                try
                {
                    pollEbsStatus(rscsToPoll, snapDfnsToPoll);
                    pollStatus.future.complete(null);
                }
                catch (Exception | ImplementationError exc)
                {
                    if (keepRunning) // otherwise, ignore exception
                    {
                        errorReporter.reportError(exc);
                    }
                    pollStatus.future.completeExceptionally(exc);
                }
            }
        }
        synchronized (syncQueueAndThread)
        {
            // do not leave waiters of a not-yet-claimed request hanging until their timeout
            if (pendingPollStatus != null)
            {
                pendingPollStatus.future.completeExceptionally(
                    new SystemServiceStopException("EbsStatusManager is shutting down")
                );
                pendingPollStatus = null;
            }
            if (Thread.currentThread().equals(thread))
            {
                thread = null;
            }
        }
    }

    private void pollEbsStatus(
        @Nullable Collection<ResourceName> rscsToPollRef,
        @Nullable Collection<SnapshotDefinition.Key> snapDfnsToPollRef
    )
    {
        // check if we have master passphrase
        if (secObjs.areAllSet())
        {
            Map<AmazonEC2, EbsRemoteIds> localIdsByAmazonClient = buildIdsByAwsClient(rscsToPollRef, snapDfnsToPollRef);
            for (Map.Entry<AmazonEC2, EbsRemoteIds> entry : localIdsByAmazonClient.entrySet())
            {
                AmazonEC2 client = entry.getKey();
                EbsRemoteIds ids = entry.getValue();

                try
                {
                    updateVolumes(client, ids.allVlmIds);
                    updateSnapshots(client, ids.allSnapIds);
                }
                catch (Exception | ImplementationError sdkExc)
                {
                    errorReporter.reportError(sdkExc);
                }
                finally
                {
                    client.shutdown();
                }
            }
        }
    }

    private Map<AmazonEC2, EbsRemoteIds> buildIdsByAwsClient(
        @Nullable Collection<ResourceName> rscsToPollRef,
        @Nullable Collection<SnapshotDefinition.Key> snapDfnsToPollRef
    )
    {
        Map<AmazonEC2, EbsRemoteIds> ret = new HashMap<>();
        try (LockGuard lockGuard = createLock())
        {
            // helper map so we do not resolve the EbsRemote for every single EbsVlmData, but first group by
            // storPools, which should reduce the EbsRemote lookups significantly.
            Map<StorPool, EbsRemoteIds> idsByStorPool = new HashMap<>();
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                if (rscsToPollRef == null || rscsToPollRef.contains(rscDfn.getName()))
                {
                    for (Resource rsc : rscDfn.getNotDeletedDiskful())
                    {
                        if (EbsUtils.isEbs(rsc))
                        {
                            addAllEbsData(
                                rsc,
                                ebsVlm -> idsByStorPool.computeIfAbsent(
                                    ebsVlm.getStorPool(),
                                    ignored -> new EbsRemoteIds()
                                )
                                    .addVlm(ebsVlm)
                            );
                        }
                    }
                }
                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns())
                {
                    if (snapDfnsToPollRef == null || snapDfnsToPollRef.contains(snapDfn.getSnapDfnKey()))
                    {
                        for (Snapshot snap : snapDfn.getAllNotDeletingSnapshots())
                        {
                            if (EbsUtils.isEbs(snap))
                            {
                                addAllEbsData(
                                    snap,
                                    ebsSnapVlm -> idsByStorPool.computeIfAbsent(
                                        ebsSnapVlm.getStorPool(),
                                        ignored -> new EbsRemoteIds()
                                    )
                                        .addSnapVlm(ebsSnapVlm)
                                );
                            }
                        }
                    }
                }
            }
            Map<EbsRemote, AmazonEC2> ebsRemoteToClient = new HashMap<>();
            for (Map.Entry<StorPool, EbsRemoteIds> entry : idsByStorPool.entrySet())
            {
                StorPool sp = entry.getKey();
                try
                {
                    EbsRemote ebsRemote = EbsUtils.getEbsRemote(
                        remoteMap,
                        sp,
                        sysCfgRepo.getStltConfForView()
                    );
                    // use the same client for the same ebsRemote so we can group the EbsRemoteIds properly
                    @Nullable AmazonEC2 client = ebsRemoteToClient.computeIfAbsent(ebsRemote, this::getClient);
                    if (client != null)
                    {
                        ret.computeIfAbsent(client, ignore -> new EbsRemoteIds())
                            .mergeWith(entry.getValue());
                    }
                }
                catch (Exception | ImplementationError exc)
                {
                    // do not let one exc / implError cancel all requests, if we have multiple ebsRemotes for example
                    errorReporter.reportError(exc, null, "Exception/Error occurred while processing " + sp.getKey());
                }
            }
        }
        return ret;
    }

    private LockGuard createLock()
    {
        return lockGuardFactory.create().read(LockObj.RSC_DFN_MAP, LockObj.REMOTE_MAP).build();
    }

    private void updateVolumes(AmazonEC2 client, Map<String, EbsData<Resource>> vlmsMapRef)
    {
        if (!vlmsMapRef.isEmpty())
        {
            final ArrayList<DescribeVolumesResult> descrVlmResultList = new ArrayList<>();
            final Map<String, VolumeModification> vlmModByEbsId = new HashMap<>();

            // send requests and gather all results in descrVlmResultList and vlmModByEbsId
            gatherPagedVolumeDescriptions(client, vlmsMapRef.keySet(), descrVlmResultList, vlmModByEbsId);

            // process the above gathered results
            try (LockGuard lg = createLock())
            {
                for (DescribeVolumesResult describeVolumesResult : descrVlmResultList)
                {
                    for (com.amazonaws.services.ec2.model.Volume amaVlm : describeVolumesResult.getVolumes())
                    {
                        @Nullable EbsData<Resource> vlmData = vlmsMapRef.get(amaVlm.getVolumeId());

                        // vlmData might be null... might be a linstor-external EBS. noop
                        if (vlmData != null)
                        {
                            updateLinstorRscStatesFromAmazonState(
                                amaVlm,
                                vlmData,
                                vlmModByEbsId.get(amaVlm.getVolumeId())
                            );
                        }
                    }
                }
            }
        }
    }

    private void gatherPagedVolumeDescriptions(
        AmazonEC2 clientRef,
        Set<String> keySetRef,
        ArrayList<DescribeVolumesResult> descrVlmResultListRef,
        Map<String, VolumeModification> vlmModByEbsIdRef
    )
    {
        final ArrayList<String> vlmIdList = new ArrayList<>(keySetRef);
        final int vlmIdListSize = vlmIdList.size();
        final int vlmIdListPages = vlmIdListSize / MAX_ENTRIES_PER_PAGE;
        for (int page = 0; page <= vlmIdListPages; page++)
        {
            List<String> currentVlmIdList = vlmIdList.subList(
                page * MAX_ENTRIES_PER_PAGE,
                Math.min((page + 1) * MAX_ENTRIES_PER_PAGE, vlmIdListSize)
            );

            Filter awsVlmIdFilter = new Filter("volume-id").withValues(currentVlmIdList);

            @Nullable String nextToken = null;
            do
            {
                DescribeVolumesResult describeVolumesResult = clientRef.describeVolumes(
                    new DescribeVolumesRequest()
                        // DO NOT use .withVolumeIds since that will throw NotFound exception if a requested VlmId no
                        // longer exist. This can easily be the case in an async setup (i.e. if LINSTOR is just about to
                        // delete the EBS volume while this code runs concurrently)
                        .withFilters(awsVlmIdFilter)
                        .withNextToken(nextToken)
                );
                descrVlmResultListRef.add(describeVolumesResult);
                nextToken = describeVolumesResult.getNextToken();
            }
            while (nextToken != null && !nextToken.isEmpty());

            nextToken = null;
            do
            {
                DescribeVolumesModificationsResult describeVolumesModifications = clientRef
                    .describeVolumesModifications(
                        new DescribeVolumesModificationsRequest()
                            // DO NOT use .withVolumeIds since that will throw NotFound exception if a requested VlmId
                            // no longer exist. This can easily be the case in an async setup (i.e. if LINSTOR is just
                            // about to delete the EBS volume while this code runs concurrently)
                            .withFilters(awsVlmIdFilter)
                            .withNextToken(nextToken)
                    );
                nextToken = describeVolumesModifications.getNextToken();
                for (VolumeModification vlmMod : describeVolumesModifications.getVolumesModifications())
                {
                    vlmModByEbsIdRef.put(vlmMod.getVolumeId(), vlmMod);
                }
            }
            while (nextToken != null && !nextToken.isEmpty());
        }
    }

    /*
     * https://docs.amazonaws.cn/en_us/AWSEC2/latest/UserGuide/ebs-describing-volumes.html
     *
     * describeVolumes.state:
     * "in-use" / "available" / "creating" / "deleting" / "deleted" / "error"
     * describeVolumeModifications.modificationState(if exists):
     * "" / "optimizing"
     * describeVolumeModifications.progress (if exists):
     * "0" / ... / "99"
     */
    private void updateLinstorRscStatesFromAmazonState(
        com.amazonaws.services.ec2.model.Volume amaVlm,
        EbsData<Resource> vlmData,
        @Nullable VolumeModification vlmMod // vlm might not have been modified
    )
    {
        AbsVolume<Resource> vlm = vlmData.getVolume();
        if (!vlm.isDeleted()) // vlm might have been deleted since last time we had the read lock
        {
            // no need to check rsc for deleted. If the volume was not deleted, rsc cannot be
            // deleted same is true for node.
            Resource rsc = vlm.getAbsResource();
            Peer peer = rsc.getNode().getPeer();
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

    private void updateSnapshots(AmazonEC2 client, Map<String, EbsData<Snapshot>> snapMapRef)
    {
        if (!snapMapRef.isEmpty())
        {
            /*
             * https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-snapshots.html
             *
             * describeSnap.state (Strings from com.amazonaws.services.ec2.model.SnapshotState):
             * "pending" / "completed" / "recoverable" / "recovering" / "error"
             * describeSnap.progress:
             * "0%" / ... / "99%"
             */

            // send requests and gather all results in descrSnapshotResultList
            List<DescribeSnapshotsResult> descrSnapResultList = gatherPagedSnapshotDescription(
                client,
                snapMapRef.keySet()
            );

            // process the above gathered results
            try (LockGuard lg = createLock())
            {
                for (DescribeSnapshotsResult describeSnapResult : descrSnapResultList)
                {
                    for (com.amazonaws.services.ec2.model.Snapshot amaSnap : describeSnapResult.getSnapshots())
                    {
                        @Nullable EbsData<Snapshot> snapVlmData = snapMapRef.get(amaSnap.getSnapshotId());

                        // snapVlmData might be null... might be a linstor-external EBS snapshot. noop
                        if (snapVlmData != null)
                        {
                            updateLinstorSnapStateFromAmazonState(amaSnap, snapVlmData);
                        }
                    }
                }
            }
        }
    }

    private void updateLinstorSnapStateFromAmazonState(
        com.amazonaws.services.ec2.model.Snapshot amaSnap,
        EbsData<Snapshot> snapVlmData
    )
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
            snapVlm.setState(diskState);
        }
    }

    private List<DescribeSnapshotsResult> gatherPagedSnapshotDescription(AmazonEC2 clientRef, Set<String> keySetRef)
    {
        List<DescribeSnapshotsResult> ret = new ArrayList<>();
        ArrayList<String> snapIdList = new ArrayList<>(keySetRef);
        final int snapIdListSize = snapIdList.size();
        final int snapIdListPages = snapIdListSize / MAX_ENTRIES_PER_PAGE;
        for (int page = 0; page <= snapIdListPages; page++)
        {
            List<String> currentSnapIdList = snapIdList.subList(
                page * MAX_ENTRIES_PER_PAGE,
                Math.min((page + 1) * MAX_ENTRIES_PER_PAGE, snapIdListSize)
            );

            Filter awsSnapIdFilter = new Filter("snapshot-id").withValues(currentSnapIdList);
            @Nullable String nextToken = null;
            do
            {
                DescribeSnapshotsResult describeSnapshots = clientRef.describeSnapshots(
                    new DescribeSnapshotsRequest()
                        // DO NOT use .withSnapshotIds since that will throw NotFound exception if a requested SnapId
                        // no longer exist. This can easily be the case in an async setup (i.e. if LINSTOR is just
                        // about to delete the EBS snapshot while this code runs concurrently)
                        .withFilters(awsSnapIdFilter)
                        .withNextToken(nextToken)
                );
                ret.add(describeSnapshots);
                nextToken = describeSnapshots.getNextToken();
            }
            while (nextToken != null && !nextToken.isEmpty());
        }
        return ret;
    }

    private @Nullable AmazonEC2 getClient(EbsRemote remoteRef)
    {
        AmazonEC2 client;
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.REMOTE_MAP))
        {
            client = AmazonEC2ClientBuilder.standard()
                .withEndpointConfiguration(
                    new EndpointConfiguration(
                        remoteRef.getUrl().toString(),
                        remoteRef.getRegion()
                    )
                )
                .withCredentials(
                    new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                            remoteRef.getDecryptedAccessKey(),
                            remoteRef.getDecryptedSecretKey()
                        )
                    )
                )
                .build();
        }
        return client;
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

    private static class PollStatus
    {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        @Nullable Collection<ResourceName> rscsToPoll;
        @Nullable Collection<SnapshotDefinition.Key> snapDfnsToPoll;

        PollStatus(
            @Nullable Collection<ResourceName> rscDfnsToPollRef,
            @Nullable Collection<SnapshotDefinition.Key> snapDfnKeysToPollRef
        )
        {
            // defensive copies, callers might pass immutable or reused collections
            rscsToPoll = rscDfnsToPollRef == null ? null : new HashSet<>(rscDfnsToPollRef);
            snapDfnsToPoll = snapDfnKeysToPollRef == null ?
                null :
                new HashSet<>(snapDfnKeysToPollRef);
        }

        void merge(
            @Nullable Collection<ResourceName> rscDfnsToPollRef,
            @Nullable Collection<SnapshotDefinition.Key> snapDfnKeysToPollRef
        )
        {
            // if local field is already null we do not need to merge anything. null will be interpreted as "all"
            if (rscsToPoll != null)
            {
                if (rscDfnsToPollRef == null)
                {
                    rscsToPoll = null; // all
                }
                else
                {
                    rscsToPoll.addAll(rscDfnsToPollRef);
                }
            }

            if (snapDfnsToPoll != null)
            {
                if (snapDfnKeysToPollRef == null)
                {
                    snapDfnsToPoll = null; // all
                }
                else
                {
                    snapDfnsToPoll.addAll(snapDfnKeysToPollRef);
                }
            }
        }

        @Nullable
        Collection<ResourceName> copyRscsToPoll()
        {
            return rscsToPoll == null ? null : new HashSet<>(rscsToPoll);
        }

        @Nullable
        Collection<SnapshotDefinition.Key> copySnapDfnsToPoll()
        {
            return snapDfnsToPoll == null ? null : new HashSet<>(snapDfnsToPoll);
        }
    }

    private static class EbsRemoteIds
    {
        private final Map<String, EbsData<Resource>> allVlmIds;
        private final Map<String, EbsData<Snapshot>> allSnapIds;

        EbsRemoteIds()
        {
            allVlmIds = new HashMap<>();
            allSnapIds = new HashMap<>();
        }

        /**
         * Stores the given EbsData iff it was already initialized (i.e. has an EBS ID in the props)
         */
        void addVlm(EbsData<Resource> vlmDataRef)
        {
            @Nullable String ebsVlmId = EbsUtils.getEbsVlmId(vlmDataRef);
            if (ebsVlmId != null)
            {
                allVlmIds.put(ebsVlmId, vlmDataRef);
            }
        }

        /**
         * Stores the given EbsData iff it was already initialized (i.e. has an EBS ID in the props)
         */
        void addSnapVlm(EbsData<Snapshot> snapVlmDataRef)
        {
            @Nullable String ebsSnapId = EbsUtils.getEbsSnapId(snapVlmDataRef);
            if (ebsSnapId != null)
            {
                allSnapIds.put(ebsSnapId, snapVlmDataRef);
            }
        }

        void mergeWith(EbsRemoteIds otherRef)
        {
            allVlmIds.putAll(otherRef.allVlmIds);
            allSnapIds.putAll(otherRef.allSnapIds);
        }
    }
}
