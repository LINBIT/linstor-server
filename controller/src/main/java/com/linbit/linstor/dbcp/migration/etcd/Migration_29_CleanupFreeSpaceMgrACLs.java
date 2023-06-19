package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Map.Entry;
import java.util.TreeMap;

@EtcdMigration(
    description = "Cleanup freespacemgr ACL entries",
    version = 56
)
public class Migration_29_CleanupFreeSpaceMgrACLs extends BaseEtcdMigration
{
    private static final String BASE_KEY_ACL = "SEC_ACL_MAP//freespacemgrs/";
    private static final String BASE_KEY_OBJ_PROT = "SEC_OBJECT_PROTECTION//freespacemgrs/";
    private static final String TBL_SP = "NODE_STOR_POOL/";
    private static final String CLM_FSM_DSP_NAME = "/FREE_SPACE_MGR_DSP_NAME";
    private static final String CLM_FSM_NAME = "/FREE_SPACE_MGR_NAME";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        // remove objProt + ACL entries for FSM
        tx.delete(prefix + BASE_KEY_ACL, true);
        tx.delete(prefix + BASE_KEY_OBJ_PROT, true);

        // storage pools still have FSM names, containing a now invalid ":" (was changed to ";")
        TreeMap<String, String> spMap = tx.get(prefix + TBL_SP, true);
        for (Entry<String, String> entry : spMap.entrySet())
        {
            String key = entry.getKey();
            String val = entry.getValue();
            if ((key.endsWith(CLM_FSM_DSP_NAME) || key.endsWith(CLM_FSM_NAME)) && val.contains(":"))
            {
                val = val.replace(":", ";");
                tx.put(key, val);
            }
        }
    }
}
