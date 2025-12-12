package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.BackgroundRunner.RunConfig;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemoteControllerFactory;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CopySnapsHelper
{
    private static final String LOCAL_REMOTE = "local-remote-generated-by-linstor";
    private static final String LOCALHOST = "localhost";

    private final SystemConfRepository systemConfRepository;
    private final RemoteRepository remoteRepository;
    private final LinstorRemoteControllerFactory linstorRemoteFactory;
    private final AccessContext apiCtx;
    private final LockGuardFactory lockGuardFactory;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlBackupL2LSrcApiCallHandler backupSrcApiHandler;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandler;
    private final BackgroundRunner backgroundRunner;
    private Provider<Peer> peerProvider;

    @Inject
    public CopySnapsHelper(
        SystemConfRepository systemConfRepositoryRef,
        RemoteRepository remoteRepositoryRef,
        LinstorRemoteControllerFactory linstorRemoteFactoryRef,
        @SystemContext AccessContext apiCtxRef,
        LockGuardFactory lockGuardFactoryRef,
        ScopeRunner scopeRunnerRef,
        ResponseConverter responseConverterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlBackupL2LSrcApiCallHandler backupSrcApiHandlerRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandlerRef,
        BackgroundRunner backgroundRunnerRef,
        Provider<Peer> peerProviderRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        remoteRepository = remoteRepositoryRef;
        linstorRemoteFactory = linstorRemoteFactoryRef;
        apiCtx = apiCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
        scopeRunner = scopeRunnerRef;
        responseConverter = responseConverterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        backupSrcApiHandler = backupSrcApiHandlerRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlSnapshotDeleteApiCallHandler = ctrlSnapshotDeleteApiCallHandlerRef;
        backgroundRunner = backgroundRunnerRef;
        peerProvider = peerProviderRef;
    }

    /**
     * Returns a pair of fluxes. ObjA will be the flux that starts the shipment while objB will be the flux
     * that gets completed once the shipment finishes.
     *
     * @param rscNameRef
     * @param dstNodeNameRef
     * @param snapDfnRef
     * @return
     */
    public Flux<ApiCallRc> getCopyFlux(
        SnapshotDefinition snapDfnRef,
        String dstNodeNameRef,
        ResponseContext context,
        boolean copySnapsForEvac
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "CopySnapshot",
            lockGuardFactory.buildDeferred(
                LockType.WRITE,
                LockObj.REMOTE_MAP,
                LockObj.NODES_MAP,
                LockObj.RSC_DFN_MAP
            ),
            () -> getCopyFluxInTransaction(
                snapDfnRef,
                dstNodeNameRef,
                true,
                copySnapsForEvac,
                true
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> getCopyFluxInTransaction(
        SnapshotDefinition snapDfnRef,
        String dstNodeNameRef,
        boolean runlocalRemoteExistsCheckRef,
        boolean copySnapsForEvac,
        boolean commit
    )
    {
        if (runlocalRemoteExistsCheckRef)
        {
            try
            {
                getOrCreateLocalRemote(); // just ensure the local remote exists
                if (commit)
                {
                    ctrlTransactionHelper.commit();
                }
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
        String rscNameRef = snapDfnRef.getResourceName().displayValue;

        return backupSrcApiHandler.shipBackup(
            null,
            rscNameRef,
            LOCAL_REMOTE,
            rscNameRef,
            dstNodeNameRef,
            null,
            null,
            null,
            null,
            snapDfnRef.getName().displayValue,
            true,
            false,
            null,
            true,
            true,
            false,
            copySnapsForEvac
        );
    }

    public void deleteSnapAfterShipmentSent(Snapshot snapToDeleteRef, boolean commit)
    {

        // this flux is just a side-effect to cleanup once the shipment is done.
        // we DO NOT want to wait with the initial flux until the entire shipment is finished.
        final NodeName nodeName = snapToDeleteRef.getNodeName();
        final ResourceName rscName = snapToDeleteRef.getResourceName();
        final SnapshotName snapName = snapToDeleteRef.getSnapshotName();

        RunConfig<ApiCallRc> runCfg = new RunConfig<>(
            String.format(
                "Cleanup snapshot '%s' of resource '%s' of node %s after copy",
                snapName.displayValue,
                rscName.displayValue,
                nodeName.displayValue
            ),
            getWaitForShipToLocalhostDoneFlux(snapToDeleteRef.getSnapshotDefinition(), commit)
                .concatWith(
                    ctrlSnapshotDeleteApiCallHandler.deleteSnapshot(
                        rscName.displayValue,
                        snapName.displayValue,
                        Collections.singletonList(nodeName.displayValue)
                    )
                ),
            apiCtx,
            Collections.emptyList(),
            true
        );
        runCfg.putSubscriberContext(Peer.class, peerProvider.get());
        backgroundRunner.runInBackground(runCfg);
    }

    public Flux<ApiCallRc> getCopyFlux(
        Set<Resource> deployedResourcesRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef,
        ResponseContext context,
        boolean copySnapsForEvac
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Create linstor remote",
            lockGuardFactory.buildDeferred(
                LockType.WRITE,
                LockObj.REMOTE_MAP,
                LockObj.NODES_MAP,
                LockObj.RSC_DFN_MAP
            ),
            () -> getCopyFluxInTransaction(
                deployedResourcesRef,
                copyAllSnapsRef,
                snapNamesToCopyRef,
                context,
                copySnapsForEvac
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> getCopyFluxInTransaction(
        Set<Resource> deployedResourcesRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef,
        ResponseContext context,
        boolean copySnapsForEvac
    )
    {
        Flux<ApiCallRc> ret = Flux.empty();
        try
        {
            boolean copyAllSnaps = copyAllSnapsRef;
            Set<String> upperSnapNames = new HashSet<>();
            if (!snapNamesToCopyRef.isEmpty())
            {
                for (String snapName : snapNamesToCopyRef)
                {
                    upperSnapNames.add(snapName.toUpperCase());
                }
            }
            getOrCreateLocalRemote(); // just ensure the local remote exists
            ctrlTransactionHelper.commit();
            for (Resource rsc : deployedResourcesRef)
            {
                ResourceDefinition rscDfn = rsc.getResourceDefinition();
                PriorityProps prioProps = new PriorityProps(
                    rscDfn.getProps(apiCtx),
                    rscDfn.getResourceGroup().getProps(apiCtx),
                    systemConfRepository.getCtrlConfForView(apiCtx)
                );
                String copyAllSnapsStr = prioProps.getProp(ApiConsts.KEY_COPY_ALL_SNAPS);
                if (copyAllSnapsStr != null)
                {
                    copyAllSnaps = Boolean.parseBoolean(copyAllSnapsStr);
                }
                String targetNodeNameStr = rsc.getNode().getName().displayValue;
                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(apiCtx))
                {
                    if (copyAllSnaps || upperSnapNames.contains(snapDfn.getName().value))
                    {
                        ret = ret.concatWith(
                            getCopyFlux(
                                snapDfn,
                                targetNodeNameStr,
                                context,
                                copySnapsForEvac
                            )
                        );
                    }
                }
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return ret;
    }

    /**
     * This method returns a linstor remote that targets the local cluster. This needs to happen within a transaction.
     *
     * @return
     *
     * @throws InvalidKeyException
     * @throws AccessDeniedException
     * @throws InvalidNameException
     * @throws LinStorDataAlreadyExistsException
     * @throws DatabaseException
     */
    private LinstorRemote getOrCreateLocalRemote() throws DatabaseException
    {
        @Nullable LinstorRemote ret = null;
        try
        {
            RemoteName remoteName = new RemoteName(LOCAL_REMOTE);
            @Nullable AbsRemote localRemote = remoteRepository.get(apiCtx, remoteName);
            if (localRemote == null)
            {
                @Nullable String clusterIdStr = systemConfRepository.getCtrlConfForView(apiCtx)
                    .getProp(
                        InternalApiConsts.KEY_CLUSTER_LOCAL_ID,
                        ApiConsts.NAMESPC_CLUSTER
                    );

                if (clusterIdStr == null)
                {
                    throw new ImplementationError("cluster-id is not allowed to be null");
                }
                final UUID localClusterUuid = UUID.fromString(clusterIdStr);
                for (AbsRemote remote : remoteRepository.getMapForView(apiCtx).values())
                {
                    if (remote instanceof LinstorRemote)
                    {
                        LinstorRemote linstorRemote = (LinstorRemote) remote;
                        if (localClusterUuid.equals(linstorRemote.getClusterId(apiCtx)))
                        {
                            ret = linstorRemote;
                        }
                    }
                }
                if (ret == null)
                {
                    ret = linstorRemoteFactory.create(
                        apiCtx,
                        remoteName,
                        CtrlRemoteApiCallHandler.createUrlWithDefaults(LOCALHOST),
                        null,
                        localClusterUuid
                    );
                    remoteRepository.put(apiCtx, ret);
                }
            }
            else
            {
                ret = (LinstorRemote) localRemote;
            }
        }
        catch (LinStorDataAlreadyExistsException | AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    public Flux<ApiCallRc> getWaitForShipToLocalhostDoneFlux(SnapshotDefinition snapDfnRef, boolean commit)
    {
        return getWaitForShipToLocalhostDoneFlux(snapDfnRef, true, commit);
    }

    public Flux<ApiCallRc> getWaitForShipToLocalhostDoneFlux(
        SnapshotDefinition snapDfnRef,
        boolean runLocalRemoteExistsCheckRef,
        boolean commit
    )
    {
        if (runLocalRemoteExistsCheckRef)
        {
            try
            {
                getOrCreateLocalRemote();
                if (commit)
                {
                    ctrlTransactionHelper.commit();
                }
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
        return getWaitForShipDoneFlux(snapDfnRef, LOCAL_REMOTE);
    }

    public Flux<ApiCallRc> getWaitForShipDoneFlux(SnapshotDefinition snapDfnRef, String remoteNameStrRef)
    {
        return backupInfoMgr.registerWaitForShipSentDoneFlux(snapDfnRef, remoteNameStrRef);
    }

    public Flux<ApiCallRc> getWaitForShipReceiveDoneFlux(Snapshot snapRef)
    {
        return backupInfoMgr.registerWaitForShipReceiveDoneFlux(snapRef);
    }
}
