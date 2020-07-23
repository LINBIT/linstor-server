package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@EtcdMigration(
    description = "Move all storage pool name properties into a simple single one",
    version = 38
)
public class Migration_11_Disable_PlainSSLConnector extends BaseEtcdMigration
{
    @Override
    public void migrate(EtcdTransaction tx) throws Exception
    {
        tx.put("/LINSTOR/PROPS_CONTAINERS//CTRLCFG:netcom/PlainConnector/bindaddress", "");
        tx.put("/LINSTOR/PROPS_CONTAINERS//CTRLCFG:netcom/SslConnector/bindaddress", "");
    }
}
