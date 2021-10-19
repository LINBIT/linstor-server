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
    private final int emptyNamespaceLength;

    @Inject
    public PropsConETCDDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        errorReporter = errorReporterRef;

        emptyNamespaceLength = getEtcdKey("", null).length();
    }

    @Override
    public Map<String, String> loadAll(String instanceName) throws DatabaseException
    {
        errorReporter.logTrace("Loading properties for instance %s", getId(instanceName));
        return loadAllImpl(instanceName);
    }

    private Map<String, String> loadAllImpl(String instanceName) throws DatabaseException
    {
        String etcdNamespace = getEtcdKey(instanceName, null);
        Map<String, String> etcdMap = namespace(etcdNamespace).get(true);

        final int propsKeyStart = etcdNamespace.length() + EtcdUtils.PK_DELIMITER.length();

        /*
         * NOTE: the ETCD keys for props is a combination of the instance name and the actual property key.
         * however, if one instance name (for example "a") is a substring of another instance name (i.e. "ab")
         * the above gathered `etcdMap` for "a" will also contain all keys of "ab".
         * Therefore we need an additional check while iterating over the properties if the current composed
         * key really corresponds to our instanceName
         */

        Map<String, String> propsMap = new TreeMap<>();
        for (Entry<String, String> entry : etcdMap.entrySet())
        {
            String composedKey = entry.getKey();
            String instName = composedKey.substring(
                emptyNamespaceLength,
                composedKey.lastIndexOf(EtcdUtils.PK_DELIMITER)
            );

            if (instName.equals(instanceName))
            {
                String propsKey = composedKey.substring(propsKeyStart);
                propsMap.put(
                    propsKey,
                    entry.getValue()
                );
            }
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

        /*
         * Do NOT use recursive delete here as that might cause a duplicated key in etcd-tx exception when doing
         * something like
         * props.clear();
         * props.setProp(...);
         */

        // as the driver currently does not know which keys to delete, unfortunately we have to load them again
        remove(instanceName, loadAll(instanceName).keySet());
    }
}
