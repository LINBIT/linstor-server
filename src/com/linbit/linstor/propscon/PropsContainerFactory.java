package com.linbit.linstor.propscon;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorSqlRuntimeException;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.Map;

public class PropsContainerFactory
{
    private final PropsConDatabaseDriver dbDriver;

    @Inject
    public PropsContainerFactory(PropsConDatabaseDriver dbDriverRef)
    {
        dbDriver = dbDriverRef;
    }

    public PropsContainer getInstance(String instanceName, TransactionMgr transMgr) throws SQLException
    {
        PropsContainer container;
        try
        {
            container = new PropsContainer(null, null, dbDriver);
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

        if (transMgr != null)
        {
            container.setConnection(transMgr);

            try
            {
                Map<String, String> loadedProps = dbDriver.load(instanceName, transMgr);
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
