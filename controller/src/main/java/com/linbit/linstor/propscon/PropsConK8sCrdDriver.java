package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class PropsConK8sCrdDriver implements PropsConDatabaseDriver
{
    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    @Inject
    public PropsConK8sCrdDriver(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrK8sCrd> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    public Map<String, String> loadAll(String instanceName) throws DatabaseException
    {
        errorReporter.logTrace("Loading properties for instance %s", getId(instanceName));

        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        HashMap<String, GenCrdCurrent.PropsContainersSpec> map = tx.get(
            GeneratedDatabaseTables.PROPS_CONTAINERS,
            propEntry -> propEntry.propsInstance.equals(instanceName)
        );

        Map<String, String> propsMap = new TreeMap<>();
        for (GenCrdCurrent.PropsContainersSpec entrySpec : map.values())
        {
            propsMap.put(entrySpec.propKey, entrySpec.propValue);
        }
        return propsMap;
    }

    @Override
    public void persist(String instanceName, String key, String value, boolean isNew) throws DatabaseException
    {
        errorReporter.logTrace("Storing property %s", getId(instanceName, key, value));
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.update(
            GeneratedDatabaseTables.PROPS_CONTAINERS,
            GenCrdCurrent.createPropsContainers(instanceName, key, value)
        );
    }

    @Override
    public void remove(String instanceName, String key) throws DatabaseException
    {
        errorReporter.logTrace("Removing property %s", getId(instanceName, key));

        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        tx.delete(
            GeneratedDatabaseTables.PROPS_CONTAINERS,
            GenCrdCurrent.createPropsContainers(instanceName, key, null)
        );
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

        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();

        HashMap<String, GenCrdCurrent.PropsContainersSpec> map = tx.get(
            GeneratedDatabaseTables.PROPS_CONTAINERS,
            propEntry -> propEntry.propsInstance.equals(instanceName)
        );

        for (GenCrdCurrent.PropsContainersSpec propsContainersSpec : map.values())
        {
            tx.delete(
                GeneratedDatabaseTables.PROPS_CONTAINERS,
                new GenCrdCurrent.PropsContainers(propsContainersSpec)
            );
        }
    }
}
