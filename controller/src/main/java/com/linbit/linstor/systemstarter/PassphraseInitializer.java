package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import com.google.inject.Inject;

public class PassphraseInitializer implements StartupInitializer
{
    private CtrlConfig ctrlCfg;
    private EncryptionHelper encHelper;
    private AccessContext accCtx;
    private LinStorScope apiCallScope;
    private TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public PassphraseInitializer(
        CtrlConfig ctrlCfgRef,
        EncryptionHelper encHelperRef,
        @SystemContext
        AccessContext accCtxRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        ctrlCfg = ctrlCfgRef;
        encHelper = encHelperRef;
        accCtx = accCtxRef;
        apiCallScope = apiCallScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    @Override
    public void initialize() throws SystemServiceStartException
    {
        apiCallScope.enter();

        try
        {
            TransactionMgrUtil.seedTransactionMgr(apiCallScope, transactionMgrGenerator.startTransaction());
            Props namespace = encHelper.getEncryptedNamespace(accCtx);
            if (namespace == null || namespace.isEmpty())
            {
                byte[] masterKey = encHelper.generateSecret();
                encHelper.setPassphraseImpl(
                    ctrlCfg.getMasterPassphrase(),
                    masterKey,
                    accCtx
                );
                // setPassphraseImpl sets the props in this namespace; to ensure they are there, get it again
                namespace = encHelper.getEncryptedNamespace(accCtx);
                setCryptKey(masterKey, namespace);
            }
            else
            {
                byte[] decryptedMasterKey = encHelper.getDecryptedMasterKey(namespace, ctrlCfg.getMasterPassphrase());
                setCryptKey(decryptedMasterKey, namespace);
            }
        }
        catch (Throwable exc)
        {
            throw new SystemServiceStartException("Automatic injection of passphrase failed", exc, true);
        }
        finally
        {
            apiCallScope.exit();
        }
    }

    private void setCryptKey(byte[] masterKeyRef, Props namespaceRef)
    {
        encHelper.setCryptKey(
            masterKeyRef,
            namespaceRef,
            false // do not update satellites before Auth (-> leads to unauthorized message -> error on stlt)
            // cryptKey is part of the FullSync anyways
        );
    }
}
