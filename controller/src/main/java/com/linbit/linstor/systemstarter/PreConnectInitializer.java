package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.RetryResourcesTask;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PreConnectInitializer implements StartupInitializer
{
    private final ErrorReporter errorReporter;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final AccessContext sysCtx;
    private final LinStorScope apiCallScope;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final CtrlSnapshotDeleteApiCallHandler snapDelApiCallHandler;
    private final RetryResourcesTask retryResourceTask;

    @Inject
    public PreConnectInitializer(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        CtrlSnapshotDeleteApiCallHandler snapDelApiCallHandlerRef,
        RetryResourcesTask retryResourceTaskRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        rscDfnRepo = rscDfnRepoRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        apiCallScope = apiCallScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        snapDelApiCallHandler = snapDelApiCallHandlerRef;
        retryResourceTask = retryResourceTaskRef;
    }

    @Override
    public void initialize()
        throws InitializationException, AccessDeniedException, DatabaseException, SystemServiceStartException
    {
        try (LinStorScope.ScopeAutoCloseable close = apiCallScope.enter())
        {
            TransactionMgrUtil.seedTransactionMgr(apiCallScope, transactionMgrGenerator.startTransaction());

            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(sysCtx).values())
            {
                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(sysCtx))
                {
                    if (
                        snapDfn.getFlags().isSomeSet(
                            sysCtx,
                            SnapshotDefinition.Flags.SHIPPING,
                            SnapshotDefinition.Flags.SHIPPING_CLEANUP
                        )
                    )
                    {
                        if (!snapDfn.getFlags().isSet(sysCtx, SnapshotDefinition.Flags.BACKUP))
                        {
                            snapDfn.getFlags().enableFlags(sysCtx, SnapshotDefinition.Flags.DELETE);
                            for (Snapshot snap : snapDfn.getAllSnapshots(sysCtx))
                            {
                                snap.getFlags().enableFlags(sysCtx, Snapshot.Flags.DELETE);

                                Resource rsc = rscDfn.getResource(sysCtx, snap.getNodeName());
                                if (rsc != null)
                                {
                                    retryResourceTask.add(
                                        rsc,
                                        snapDelApiCallHandler
                                            .deleteSnapshotsOnNodes(rscDfn.getName(), snapDfn.getName())
                                    );
                                }
                            }
                        }
                        else
                        {
                            snapDfn.getFlags().disableFlags(sysCtx, SnapshotDefinition.Flags.SHIPPING);
                            snapDfn.getFlags().enableFlags(sysCtx, SnapshotDefinition.Flags.SHIPPING_ABORT);
                            for (Snapshot snap : snapDfn.getAllSnapshots(sysCtx))
                            {
                                String key = null;
                                if (snap.getFlags().isSet(sysCtx, Snapshot.Flags.BACKUP_SOURCE))
                                {
                                    // if the snapshot is the source, the remote is the target
                                    key = InternalApiConsts.KEY_BACKUP_TARGET_REMOTE;
                                }
                                else if (snap.getFlags().isSet(sysCtx, Snapshot.Flags.BACKUP_TARGET))
                                {
                                    key = InternalApiConsts.KEY_BACKUP_SRC_REMOTE;
                                }

                                if (key != null)
                                {
                                    snap.getSnapProps(sysCtx)
                                        .removeProp(key, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                                }
                            }
                            // no need to do something similar to the retryTask-stuff above, since full-sync will happen
                            // next anyways
                        }
                    }
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (Exception exc)
        {
            throw new SystemServiceStartException("Automatic cleanup after restart failed", exc, true);
        }
    }
}
