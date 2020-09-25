package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Fix incorrectly set AutoTiebreaker and AutoQuorum ctrl properties",
    version = 35
)
public class Migration_08_FixAutoTiebreakerProps extends BaseEtcdMigration
{

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        String oldAutoTieBreakerKey = prefix +
            "PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-add-quorum-tiebreaker";
        String newAutoTieBreakerKey = prefix +
            "PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-add-quorum-tiebreaker";
        String autoTieBreakerValue = tx.getFirstValue(oldAutoTieBreakerKey);

        String oldAutoQuorumKey = prefix + "PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-quorum";
        String newAutoQuorumKey = prefix + "PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-quorum";
        String autoQuorumValue = tx.getFirstValue(oldAutoQuorumKey);

        if (autoTieBreakerValue != null)
        {
            tx.delete(oldAutoTieBreakerKey, false);
            tx.put(newAutoTieBreakerKey, autoTieBreakerValue);
        }

        if (autoQuorumValue != null)
        {
            tx.delete(oldAutoQuorumKey, false);
            tx.put(newAutoQuorumKey, autoQuorumValue);
        }
    }
}
