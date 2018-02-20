package com.linbit.linstor.dbdrivers.satellite;

import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class SatellitePropDriver implements PropsConDatabaseDriver
{
    @Inject
    public SatellitePropDriver()
    {
    }

    @Override
    public Map<String, String> load(String instanceName, TransactionMgr transMgr)
    {
        return Collections.emptyMap();
    }

    @Override
    public void persist(String instanceName, String key, String value, TransactionMgr transMgr)
    {
        // no-op
    }

    @Override
    public void persist(String instanceName, Map<String, String> props, TransactionMgr transMgr)
    {
        // no-op
    }

    @Override
    public void remove(String instanceName, String key, TransactionMgr transMgr)
    {
        // no-op
    }

    @Override
    public void remove(String instanceName, Set<String> keys, TransactionMgr transMgr)
    {
        // no-op
    }

    @Override
    public void removeAll(String instanceName, TransactionMgr transMgr)
    {
        // no-op
    }
}
