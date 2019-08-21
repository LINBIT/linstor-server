package com.linbit.linstor;

import com.ibm.etcd.client.kv.KvClient;

public interface ControllerETCDDatabase extends ControllerDatabase
{
    KvClient getKvClient();
}
