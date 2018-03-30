package com.linbit.linstor.debug;

import javax.inject.Inject;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorSqlRuntimeException;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Named;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import javax.inject.Provider;

public class CmdSetConfValue extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_KEY = "KEY";
    private static final String PRM_VALUE = "VALUE";
    private static final String PRM_NAMESPACE = "NAMESPACE";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_KEY,
            "Key for the new configuration entry"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_VALUE,
            "Value for the new configuration entry"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_NAMESPACE,
            "Configuration namespace to select.\n" +
            "Note that specifying a key with an absolute path overrides the namespace selection."
        );
    }

    private final DbConnectionPool dbConnectionPool;
    private final Lock confWrLock;
    private final Props conf;
    private final ObjectProtection confProt;
    private final Provider<TransactionMgr> trnActProvider;

    @Inject
    public CmdSetConfValue(
        DbConnectionPool dbConnectionPoolRef,
        @Named(ControllerCoreModule.CTRL_CONF_LOCK) ReadWriteLock confLockRef,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props confRef,
        @Named(ControllerSecurityModule.CTRL_CONF_PROT) ObjectProtection confProtRef,
        Provider<TransactionMgr> trnActProviderRef
    )
    {
        super(
            new String[]
            {
                "SetCfgVal"
            },
            "Set configuration value",
            "Sets the value of a configuration entry.\nIf the entry does not exist, it is created.",
            PARAMETER_DESCRIPTIONS,
            null
        );

        dbConnectionPool = dbConnectionPoolRef;
        confWrLock = confLockRef.writeLock();
        conf = confRef;
        confProt = confProtRef;
        trnActProvider = trnActProviderRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        TransactionMgr transMgr = null;

        confWrLock.lock();
        try
        {
            String key = parameters.get(PRM_KEY);
            String value = parameters.get(PRM_VALUE);
            String namespace =  parameters.get(PRM_NAMESPACE);

            if (key != null && value != null)
            {
                confProt.requireAccess(accCtx, AccessType.CHANGE);

                // Commit changes to the database
                transMgr = trnActProvider.get();

                String previous = conf.setProp(key, value, namespace);

                transMgr.commit();

                StringBuilder confirmText = new StringBuilder();
                if (previous == null)
                {
                    confirmText.append("Created configuration entry");
                }
                else
                {
                    confirmText.append("Changed configuration entry");
                }
                if (namespace != null && !key.startsWith("/"))
                {
                    confirmText.append(String.format(" in namespace '%s'", namespace));
                }
                confirmText.append(String.format(": %s = %s\n", key, value));
                debugOut.print(confirmText.toString());
                debugOut.flush();
            }
            else
            {
                printMultiMissingParamError(debugErr, parameters, PRM_KEY, PRM_VALUE);
            }
        }
        catch (LinStorSqlRuntimeException sqlExc)
        {
            printError(
                debugErr,
                "The database transaction to update the configuration failed.",
                sqlExc.getCauseText(),
                null,
                null
            );
        }
        catch (InvalidKeyException invKeyExc)
        {
            printError(
                debugErr,
                "The key for the configuration entry is not valid.",
                null,
                "Reenter the command with a valid key for the configuration entry.",
                null
            );
        }
        catch (InvalidValueException invValExc)
        {
            printError(
                debugErr,
                "The value for the configuration entry is not valid.",
                null,
                "Reenter the command with a valid value for the configuration entry.",
                null
            );
        }
        catch (LinStorException dmExc)
        {
            printDmException(debugErr, dmExc);
        }
        finally
        {
            confWrLock.unlock();
            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
            if (conf != null)
            {
                conf.setConnection(null);
            }
        }
    }

    @Override
    public boolean requiresScope()
    {
        return true;
    }
}
