package com.linbit.linstor.debug;

import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorSqlRuntimeException;
import com.linbit.linstor.core.CtrlDebugControl;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

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

    public CmdSetConfValue()
    {
        super(
            new String[]
            {
                "SetCfgVal"
            },
            "Set configuration value",
            "Sets the value of a configuration entry.\nIf the entry does not exist, it is created.",
            PARAMETER_DESCRIPTIONS,
            null,
            false
        );
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
        Props conf = null;
        DbConnectionPool dbConnPool = null;
        TransactionMgr transMgr = null;
        Lock confLock = cmnDebugCtl.getConfLock().writeLock();
        confLock.lock();
        try
        {
            String key = parameters.get(PRM_KEY);
            String value = parameters.get(PRM_VALUE);
            String namespace =  parameters.get(PRM_NAMESPACE);

            if (key != null && value != null)
            {
                conf = cmnDebugCtl.getConf();
                {
                    ObjectProtection confProt = cmnDebugCtl.getConfProt();
                    if (confProt != null)
                    {
                        confProt.requireAccess(accCtx, AccessType.CHANGE);
                    }
                }

                // On the controller, commit changes to the database
                if (cmnDebugCtl instanceof CtrlDebugControl)
                {
                    CtrlDebugControl ctrlDebugCtl = (CtrlDebugControl) cmnDebugCtl;
                    dbConnPool = ctrlDebugCtl.getDbConnectionPool();
                    transMgr = new TransactionMgr(dbConnPool);
                    conf.setConnection(transMgr);
                }
                String previous = conf.setProp(key, value, namespace);

                if (transMgr != null)
                {
                    transMgr.commit();
                }
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
            confLock.unlock();

            if (dbConnPool != null && transMgr != null)
            {
                dbConnPool.returnConnection(transMgr.dbCon);
            }
            if (conf != null)
            {
                conf.setConnection(null);
            }
        }
    }
}
