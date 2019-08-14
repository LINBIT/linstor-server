package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

@SuppressWarnings({"checkstyle:typename", "checkstyle:magicnumber"})
@Migration(
    version = "2019.03.14.09.10",
    description = "Converting string-based storageDriver to enum-based providerKind"
)
public class Migration_2019_03_14_ProviderType extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        Map<String, String> update = new TreeMap<>();
        update.put("DisklessDriver", "DISKLESS");
        update.put("LvmDriver", "LVM");
        update.put("LvmThinDriver", "LVM_THIN");
        update.put("ZfsDriver", "ZFS");
        update.put("ZfsThinDriver", "ZFS_THIN");
        update.put("SwordfishTargetDriver", "SWORDFISH_TARGET");
        update.put("SwordfishInitiatorDriver", "SWORDFISH_INITIATOR");

        for (Entry<String, String> entry : update.entrySet())
        {
            SQLUtils.runSql(
                connection,
                String.format(
                    "UPDATE NODE_STOR_POOL SET DRIVER_NAME = '%s' WHERE DRIVER_NAME = '%s';",
                    entry.getValue(),
                    entry.getKey()
                )
            );
        }
    }
}
