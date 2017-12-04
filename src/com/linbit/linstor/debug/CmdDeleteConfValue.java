package com.linbit.linstor.debug;

import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorSqlRuntimeException;
import com.linbit.linstor.core.CtrlDebugControl;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

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
        catch (LinStorException dmExc)
        {
            printDmException(debugErr, dmExc);
        }
        finally
        {
            confLock.unlock();

            if (dbConnPool != null && transMgr != null)
            {
                dbConnPool.returnConnection(transMgr);
            }
            if (conf != null)
            {
                conf.setConnection(null);
            }
        }
    }
}
