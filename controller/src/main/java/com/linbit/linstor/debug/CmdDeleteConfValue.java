package com.linbit.linstor.debug;

import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
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

    private final Lock confWrLock;
    private final SystemConfRepository systemConfRepository;
    private final Provider<TransactionMgr> trnActProvider;

    @Inject
    public CmdDeleteConfValue(
        @Named(CoreModule.CTRL_CONF_LOCK) ReadWriteLock confLockRef,
        SystemConfRepository systemConfRepositoryRef,
        Provider<TransactionMgr> trnActProviderRef
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

        confWrLock = confLockRef.writeLock();
        systemConfRepository = systemConfRepositoryRef;
        trnActProvider = trnActProviderRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        Map<String, String> parameters
    )
        throws Exception
    {
        TransactionMgr transMgr = null;

        confWrLock.lock();
        try
        {
            String key = parameters.get(PRM_KEY);
            String namespace =  parameters.get(PRM_NAMESPACE);

            if (key != null)
            {
                transMgr = trnActProvider.get();

                String removed = systemConfRepository.removeCtrlProp(key, namespace);
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
        catch (LinStorDBRuntimeException dbExc)
        {
            printError(
                debugErr,
                "The database transaction to update the configuration failed.",
                dbExc.getCauseText(),
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
        catch (LinStorException lsExc)
        {
            printLsException(debugErr, lsExc);
        }
        finally
        {
            confWrLock.unlock();
            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
        }
    }

    @Override
    public boolean requiresScope()
    {
        return true;
    }
}
