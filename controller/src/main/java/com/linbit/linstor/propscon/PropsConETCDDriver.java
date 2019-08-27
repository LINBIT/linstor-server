package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.TransactionMgrETCD;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Singleton
public class PropsConETCDDriver extends BaseEtcdDriver implements PropsConDatabaseDriver
{
    private final ErrorReporter errorReporter;

    @Inject
    public PropsConETCDDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        errorReporter = errorReporterRef;
    }

    @Override
    public Map<String, String> loadAll(String instanceName) throws DatabaseException
    {
        errorReporter.logTrace("Loading properties for instance %s", getId(instanceName));
        return namespace(GeneratedDatabaseTables.PROPS_CONTAINERS, instanceName).get(true);
    }

    @Override
    public void persist(String instanceName, String key, String value) throws DatabaseException
    {
        errorReporter.logTrace("Storing property %s", getId(instanceName, key, value));
        namespace(GeneratedDatabaseTables.PROPS_CONTAINERS, instanceName)
            .put(key, value);
    }

    @Override
    public void persist(String instanceName, Map<String, String> props) throws DatabaseException
    {
        for (Entry<String, String> entry : props.entrySet())
        {
            persist(instanceName, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(String instanceName, String key) throws DatabaseException
    {
        errorReporter.logTrace("Removing property %s", getId(instanceName, key));
        namespace(EtcdUtils.buildKey(key, GeneratedDatabaseTables.PROPS_CONTAINERS, instanceName))
            .delete(false);
    }

    @Override
    public void remove(String instanceNameRef, Set<String> keysRef) throws DatabaseException
    {
        for (String key : keysRef)
        {
            remove(instanceNameRef, key);
        }
    }

    @Override
    public void removeAll(String instanceName) throws DatabaseException
    {
        errorReporter.logTrace("Removing all properties by instance %s", getId(instanceName));

        namespace(GeneratedDatabaseTables.PROPS_CONTAINERS, instanceName)
            .delete(true);
    }

    private String getId(String instanceName)
    {
        return "(InstanceName=" + instanceName + ")";
    }

    private String getId(String instanceName, String key)
    {
        return "(InstanceName=" + instanceName + " Key=" + key + ")";
    }

    private String getId(String instanceName, String key, String value)
    {
        return "(InstanceName=" + instanceName + " Key=" + key + " Value=" + value + ")";
    }
}
