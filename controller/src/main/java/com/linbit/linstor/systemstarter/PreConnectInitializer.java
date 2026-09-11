package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Iterator;

@Singleton
public class PreConnectInitializer implements StartupInitializer
{
    private final ResourceDefinitionRepository rscDfnRepo;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final LinStorScope apiCallScope;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public PreConnectInitializer(
        ResourceDefinitionRepository rscDfnRepoRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        rscDfnRepo = rscDfnRepoRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        apiCallScope = apiCallScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    @Override
    public void initialize()
        throws InitializationException, DatabaseException, SystemServiceStartException
    {
        try (LinStorScope.ScopeAutoCloseable close = apiCallScope.enter())
        {
            TransactionMgrUtil.seedTransactionMgr(apiCallScope, transactionMgrGenerator.startTransaction());

            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView().values())
            {
                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns())
                {
                    if (BackupShippingUtils.isAnyShippingInProgress(snapDfn))
                    {
                        @Nullable Props backupProps = snapDfn.getSnapDfnProps()
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
                                    InternalApiConsts.VALUE_SHIPPING
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
                                                InternalApiConsts.VALUE_SHIPPING
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
