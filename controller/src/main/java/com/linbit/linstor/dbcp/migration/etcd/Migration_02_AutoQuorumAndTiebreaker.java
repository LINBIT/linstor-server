package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

// corresponds to Migration_2019_10_01_AutoQuorumAndTieBreaker
public class Migration_02_AutoQuorumAndTiebreaker extends EtcdMigration
{
    public static void migrate(EtcdTransaction tx)
    {
        tx.put(
            "LINSTOR/PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-quorum",
            "io-error"
        );
        tx.put(
            "LINSTOR/PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-add-quorum-tiebreaker",
            "True"
        );
    }
}
