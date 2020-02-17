package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

@EtcdMigration(
    description = "Fix incorrectly set AutoTiebreaker and AutoQuorum ctrl properties",
    version = 35
)
public class Migration_08_FixAutoTiebreakerProps extends BaseEtcdMigration
{

    @Override
    public void migrate(EtcdTransaction tx) throws Exception
    {
        String oldAutoTieBreakerKey = "/LINSTOR/PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-add-quorum-tiebreaker";
        String newAutoTieBreakerKey = "/LINSTOR/PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-add-quorum-tiebreaker";
        String autoTieBreakerValue = tx.getFirstValue(oldAutoTieBreakerKey);

        String oldAutoQuorumKey = "/LINSTOR/PROPS_CONTAINER/CTRLCFG/DrbdOptions/auto-quorum";
        String newAutoQuorumKey = "/LINSTOR/PROPS_CONTAINERS//CTRLCFG:DrbdOptions/auto-quorum";
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
