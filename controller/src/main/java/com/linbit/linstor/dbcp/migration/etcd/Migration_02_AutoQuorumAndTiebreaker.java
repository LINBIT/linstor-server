package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.ControllerETCDTransactionMgr;

// corresponds to Migration_2019_10_01_AutoQuorumAndTieBreaker
public class Migration_02_AutoQuorumAndTiebreaker extends EtcdMigration
{
    public static void migrate(ControllerETCDTransactionMgr txMgr)
    {
        txMgr.getTransaction().put(
            putReq(
                "LINSTOR/PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-quorum",
                "io-error"
            )
        );
        txMgr.getTransaction().put(
            putReq(
                "LINSTOR/PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-add-quorum-tiebreaker",
                "True"
            )
        );
    }
}
