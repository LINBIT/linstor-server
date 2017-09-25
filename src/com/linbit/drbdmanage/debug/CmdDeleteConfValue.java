package com.linbit.drbdmanage.debug;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.DrbdSqlRuntimeException;
import com.linbit.drbdmanage.core.CtrlDebugControl;
import com.linbit.drbdmanage.dbcp.DbConnectionPool;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;

public class CmdDeleteConfValue extends BaseDebugCmd
{
    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();

    private static final String PRM_KEY = "KEY";
    private static final String PRM_NAMESPACE = "NAMESPACE";

    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_KEY,
            "Key of the configuration entry to delete"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_NAMESPACE,
            "Configuration namespace to select.\n" +
            "Note that specifying a key with an absolute path overrides the namespace selection."
        );
    }

    public CmdDeleteConfValue()
    {
        super(
            new String[]
            {
                "DelCfgVal"
            },
            "Deletes a configuration value",
            "Deletes an entry from the configuration.",
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
            String namespace =  parameters.get(PRM_NAMESPACE);

            if (key != null)
            {
                conf = cmnDebugCtl.getConf();

                // On the controller, commit changes to the database
                if (cmnDebugCtl instanceof CtrlDebugControl)
                {
                    CtrlDebugControl ctrlDebugCtl = (CtrlDebugControl) cmnDebugCtl;
                    dbConnPool = ctrlDebugCtl.getDbConnectionPool();
                    transMgr = new TransactionMgr(dbConnPool);
                    conf.setConnection(transMgr);
                }
                String removed = conf.removeProp(key, namespace);
                if (removed == null)
                {
                    if (namespace == null)
                    {
                        debugOut.printf("The configuration key '%s' does not exist.\n", key);
                    }
                    else
                    {
                        debugOut.printf(
                            "The configuration key '%s' does not exist in namespace '%s'.\n",
                            key, namespace
                        );
                    }
                }
                else
                {
                    if (namespace == null || key.startsWith("/"))
                    {
                        debugOut.printf("Deleted configuration key '%s'\n", key);
                    }
                    else
                    {
                        debugOut.printf("Deleted configuration key '%s' from namespace '%s'\n", key, namespace);
                    }
                }

                if (transMgr != null)
                {
                    transMgr.commit();
                }
            }
            else
            {
                printMissingParamError(debugErr, PRM_KEY);
            }
        }
        catch (DrbdSqlRuntimeException sqlExc)
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
