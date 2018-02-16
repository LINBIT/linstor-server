package com.linbit.linstor.debug;

import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorSqlRuntimeException;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;

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

    private final DbConnectionPool dbConnectionPool;
    private final ReadWriteLock confLock;
    private final Props conf;
    private final ObjectProtection confProt;

    @Inject
    public CmdDeleteConfValue(
        DbConnectionPool dbConnectionPoolRef,
        @Named(ControllerCoreModule.CTRL_CONF_LOCK) ReadWriteLock confLockRef,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props confRef,
        @Named(ControllerSecurityModule.CTRL_CONF_PROT) ObjectProtection confProtRef
    )
    {
        super(
            new String[]
            {
                "DelCfgVal"
            },
            "Deletes a configuration value",
            "Deletes an entry from the configuration.",
            PARAMETER_DESCRIPTIONS,
            null
        );

        dbConnectionPool = dbConnectionPoolRef;
        confLock = confLockRef;
        conf = confRef;
        confProt = confProtRef;
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
        confLock.writeLock().unlock();
        try
        {
            String key = parameters.get(PRM_KEY);
            String namespace =  parameters.get(PRM_NAMESPACE);

            if (key != null)
            {
                confProt.requireAccess(accCtx, AccessType.CHANGE);

                // Commit changes to the database
                transMgr = new TransactionMgr(dbConnectionPool);
                conf.setConnection(transMgr);

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

                transMgr.commit();
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
            confLock.writeLock().unlock();

            if (transMgr != null)
            {
                dbConnectionPool.returnConnection(transMgr);
            }
            if (conf != null)
            {
                conf.setConnection(null);
            }
        }
    }

}
