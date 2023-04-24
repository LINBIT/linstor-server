package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.apicallhandler.controller.CtrlResyncAfterHelper;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Inject;
import javax.inject.Singleton;



@Singleton
public class ResyncAfterInitializer implements StartupInitializer
{
    private final LinStorScope apiCallScope;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final CtrlResyncAfterHelper resyncAfterHelper;

    @Inject
    public ResyncAfterInitializer(
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        CtrlResyncAfterHelper ctrlResyncAfterHelperRef
    )
    {
        apiCallScope = apiCallScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        resyncAfterHelper = ctrlResyncAfterHelperRef;
    }

    @Override
    public void initialize() throws SystemServiceStartException
    {

        try (LinStorScope.ScopeAutoCloseable close = apiCallScope.enter())
        {
            TransactionMgr txMgr = transactionMgrGenerator.startTransaction();
            TransactionMgrUtil.seedTransactionMgr(apiCallScope, txMgr);

            resyncAfterHelper.manage();

            txMgr.commit();
        }
        catch (Throwable exc)
        {
            throw new SystemServiceStartException("ResyncAfter field setter failed", exc, true);
        }
    }
}
