package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.etcd.BaseEtcdDriver;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

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
        String etcdNamespace = getEtcdKey(instanceName, null);
        Map<String, String> etcdMap = namespace(etcdNamespace).get(true);

        final int propsKeyStart = etcdNamespace.length() + EtcdUtils.PK_DELIMITER.length();

        Map<String, String> propsMap = new TreeMap<>();
        for (Entry<String, String> entry : etcdMap.entrySet())
        {
            propsMap.put(
                entry.getKey().substring(propsKeyStart),
                entry.getValue()
            );
        }
        return propsMap;
    }

    private String getEtcdKey(String instanceName, String key)
    {
        String etcdKey = EtcdUtils.buildKey(GeneratedDatabaseTables.PROPS_CONTAINERS, instanceName);
        /*
         * buildKeys adds a trailing / at the end of our "primary key".
         * However, if key is null, we most likely need this etcdKey for recursive call, which means
         * we have to get rid of the trailing /
         * Otherwise, the instanceName and the key should be separated with : instead of /
         */
        etcdKey = etcdKey.substring(0, etcdKey.length() - EtcdUtils.PATH_DELIMITER.length());
        if (key != null)
        {
            etcdKey += EtcdUtils.PK_DELIMITER + key;
        }
        return etcdKey;
    }

    @Override
    public void persist(String instanceName, String key, String value) throws DatabaseException
    {
        errorReporter.logTrace("Storing property %s", getId(instanceName, key, value));
        /*
         * DO NOT use
         * namespace(..., instanceName).put(key, value);
         * that generates a key like
         * /LINSTOR/PROPS_CONTAINERS//$instanceName/$key = $value
         * but the loader expects all keys to have the format
         * /LINSTOR/PROPS_CONTAINERS//$instanceName:$key = $value
         * (':' instead of '/')
         */
        namespace(getEtcdKey(instanceName, key))
            .put("", value);
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
        namespace(getEtcdKey(instanceName, key))
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

        namespace(getEtcdKey(instanceName, null))
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
