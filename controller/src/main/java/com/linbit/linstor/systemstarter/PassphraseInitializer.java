package com.linbit.linstor.systemstarter;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;

import javax.inject.Singleton;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.inject.Inject;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@Singleton
public class PassphraseInitializer implements StartupInitializer
{
    private static final String SYSTEMD_CREDS_LINSTOR_MASTERPASSPHRASE_FILENAME = "linstor-masterpassphrase";
    private final ErrorReporter errorReporter;
    private CtrlConfig ctrlCfg;
    private EncryptionHelper encHelper;
    private AccessContext accCtx;
    private LinStorScope apiCallScope;
    private TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public PassphraseInitializer(
        ErrorReporter errorReporterRef,
        CtrlConfig ctrlCfgRef,
        EncryptionHelper encHelperRef,
        @SystemContext
        AccessContext accCtxRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlCfg = ctrlCfgRef;
        encHelper = encHelperRef;
        accCtx = accCtxRef;
        apiCallScope = apiCallScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
    }

    @Override
    public void initialize() throws SystemServiceStartException
    {
        // check if there is something to initialize
        @Nullable byte[] masterPassphrase = getMasterPassphraseByToml();
        if (masterPassphrase == null)
        {
            masterPassphrase = getMasterPassphraseBySystemdCredentials();
        }

        if (masterPassphrase != null)
        {
            TransactionMgr txMgr = transactionMgrGenerator.startTransaction();
            try (LinStorScope.ScopeAutoCloseable close = apiCallScope.enter())
            {
                TransactionMgrUtil.seedTransactionMgr(apiCallScope, txMgr);
                ReadOnlyProps namespace = encHelper.getEncryptedNamespace(accCtx);
                if (namespace == null || namespace.isEmpty())
                {
                    byte[] masterKey = encHelper.generateSecret();
                    encHelper.setPassphraseImpl(
                        masterPassphrase,
                        masterKey,
                        accCtx
                    );
                    // setPassphraseImpl sets the props in this namespace; to ensure they are there, get it again
                    namespace = encHelper.getEncryptedNamespace(accCtx);

                    // we can ignore the returned flux since we should be running during startup before any
                    // node-connection-attempts
                    setCryptKey(masterKey, namespace);
                }
                else
                {
                    byte[] decryptedMasterKey = encHelper.getDecryptedMasterKey(
                        namespace,
                        masterPassphrase
                    );
                    setCryptKey(decryptedMasterKey, namespace);
                }
                // setCryptKey could have changed voaltileRscData (ignoreReasons, etc...)
                txMgr.commit();
            }
            catch (Exception exc)
            {
                throw new SystemServiceStartException("Automatic injection of passphrase failed", exc, true);
            }
            finally
            {
                if (txMgr.isDirty())
                {
                    try
                    {
                        txMgr.rollback();
                    }
                    catch (TransactionException dbExc)
                    {
                        errorReporter.reportError(
                            Level.ERROR,
                            dbExc,
                            accCtx,
                            null,
                            "A database error occurred while trying to rollback"
                        );
                    }
                }
                txMgr.returnConnection();
            }
        }
        else
        {
            errorReporter.logInfo("No masterpassphrase provided");
        }
    }

    private Flux<ApiCallRc> setCryptKey(byte[] masterKeyRef, ReadOnlyProps namespaceRef)
    {
        return encHelper.setCryptKey(
            masterKeyRef,
            namespaceRef,
            false // do not update satellites before Auth (-> leads to unauthorized message -> error on stlt)
            // cryptKey is part of the FullSync anyways
        );
    }

    private @Nullable byte[] getMasterPassphraseByToml()
    {
        @Nullable byte[] ret = null;
        @Nullable String masterPassphrase = ctrlCfg.getMasterPassphrase();
        if (masterPassphrase != null)
        {
            errorReporter.logInfo("Using masterpassphrase provided by config (toml file or env)");
            ret = masterPassphrase.getBytes(StandardCharsets.UTF_8);
        }
        return ret;
    }

    private @Nullable byte[] getMasterPassphraseBySystemdCredentials() throws SystemServiceStartException
    {
        @Nullable byte[] ret = null;
        @Nullable String credentialsDir = System.getenv("CREDENTIALS_DIRECTORY");
        if (credentialsDir != null)
        {
            errorReporter.logInfo("Using masterpassphrase provided by systemd-creds");
            try
            {
                ret = Files.readAllBytes(Paths.get(credentialsDir, SYSTEMD_CREDS_LINSTOR_MASTERPASSPHRASE_FILENAME));
            }
            catch (IOException exc)
            {
                throw new SystemServiceStartException(
                    "Failed to read masterpassphrase from systemd-creds file '" +
                        SYSTEMD_CREDS_LINSTOR_MASTERPASSPHRASE_FILENAME + "'",
                    exc,
                    true
                );
            }
        }
        return ret;
    }
}
