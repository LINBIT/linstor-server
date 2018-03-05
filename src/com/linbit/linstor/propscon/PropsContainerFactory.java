package com.linbit.linstor.propscon;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorSqlRuntimeException;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.Map;

public class PropsContainerFactory
{
    private final PropsConDatabaseDriver dbDriver;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public PropsContainerFactory(
        PropsConDatabaseDriver dbDriverRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        transMgrProvider = transMgrProviderRef;
    }

    public PropsContainer getInstance(String instanceName) throws SQLException
    {
        PropsContainer container;
        try
        {
            container = new PropsContainer(null, null, dbDriver, transMgrProvider);
        }
        catch (InvalidKeyException keyExc)
        {
            // If root container creation generates an InvalidKeyException,
            // that is always a bug in the implementation
            throw new ImplementationError(
                "Root container creation generated an exception",
                keyExc
            );
        }

        container.instanceName = instanceName;

        if (transMgrProvider.get() != null)
        {
            // container.setConnection(transMgr); // TODO this should not be needed, or committed at the end
            try
            {
                Map<String, String> loadedProps = dbDriver.load(instanceName);
                for (Map.Entry<String, String> entry : loadedProps.entrySet())
                {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    PropsContainer targetContainer = container;
                    int idx = key.lastIndexOf(Props.PATH_SEPARATOR);
                    if (idx != -1)
                    {
                        targetContainer = container.ensureNamespaceExists(key.substring(0, idx));
                    }
                    String actualKey = key.substring(idx + 1);
                    String oldValue = targetContainer.getRawPropMap().put(actualKey, value);
                    if (oldValue == null)
                    {
                        targetContainer.modifySize(1);
                    }
                }
            }
            catch (InvalidKeyException invalidKeyExc)
            {
                throw new LinStorSqlRuntimeException(
                    "PropsContainer could not be loaded because a key in the database has an invalid value.",
                    invalidKeyExc
                );
            }
        }

        container.initialized();

        return container;
    }
}
