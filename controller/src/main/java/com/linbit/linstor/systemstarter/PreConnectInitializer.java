package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.RetryResourcesTask;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Iterator;

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
                    if (BackupShippingUtils.isAnyShippingInProgress(snapDfn, sysCtx))
                    {
                        @Nullable Props backupProps = snapDfn.getSnapDfnProps(sysCtx)
                            .getNamespace(ApiConsts.NAMESPC_BACKUP_SHIPPING);
                        // this should not be able to be null, since there is at least one shipping in progress, but
                        // check anyways
                        if (backupProps != null)
                        {
                            @Nullable Props dstProps = backupProps.getNamespace(InternalApiConsts.KEY_BACKUP_TARGET);
                            if (
                                dstProps != null && BackupShippingUtils.hasShippingStatus(
                                    snapDfn,
                                    null,
                                    InternalApiConsts.VALUE_SHIPPING,
                                    sysCtx
                                )
                            )
                            {
                                dstProps.setProp(
                                    InternalApiConsts.KEY_SHIPPING_STATUS,
                                    InternalApiConsts.VALUE_ABORTED
                                );
                            }
                            else
                            {
                                @Nullable Props srcProps = backupProps.getNamespace(
                                    InternalApiConsts.KEY_BACKUP_SOURCE
                                );
                                // again, it should not be possible for this to be null
                                if (srcProps != null)
                                {
                                    Iterator<String> namespcIter = srcProps.iterateNamespaces();
                                    while (namespcIter.hasNext())
                                    {
                                        String remoteName = namespcIter.next();
                                        if (
                                            BackupShippingUtils.hasShippingStatus(
                                                snapDfn,
                                                remoteName,
                                                InternalApiConsts.VALUE_SHIPPING,
                                                sysCtx
                                            )
                                        )
                                        {
                                            srcProps.setProp(
                                                InternalApiConsts.KEY_SHIPPING_STATUS,
                                                InternalApiConsts.VALUE_ABORTED,
                                                remoteName
                                            );
                                        }
                                    }
                                }
                            }
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
