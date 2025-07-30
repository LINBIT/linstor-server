package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.utils.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.UUID;

@Migration(
    version = "2023.07.25.09.00",
    description = "Restore invisible KVS"
)
public class Migration_2023_07_25_RestoreInvisbleKvs extends LinstorMigration
{
    /*
     * Migration_2022_11_14_CleanupOrphanedObjects accidentally deleted all KVS entries from the KVS table, but not
     * the props.
     * The problem was that the migration scanned for KVS with no properties, but was looking for lower-cased
     * PROPS_INSTANCE entries although the driver uppercases the PROPS_INSTANCE always.
     *
     * The props were not touched, but since the entries in the KVS table was missing,
     * "linstor key-value-store list" did not list them and "linstor key-value-store show <kvs>" also did not
     * contain any entries since they were simply not loaded from the database
     */

    private static final String TBL_PROPS = "PROPS_CONTAINERS";
    private static final String TBL_KVS = "KEY_VALUE_STORE";

    private static final String CLM_PROPS_INSTANCE = "PROPS_INSTANCE";

    private static final String CLM_KVS_NAME = "KVS_NAME";
    private static final String CLM_KVS_UUID = "UUID";
    private static final String CLM_KVS_DSP_NAME = "KVS_DSP_NAME";

    private static final String PROP_INSTANCE_PREFIX = "/KEYVALUESTORES/";

    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement selectPropsStmt = conRef.prepareStatement(
                "SELECT " + CLM_PROPS_INSTANCE + " FROM " + TBL_PROPS
            );
            PreparedStatement insertKvsStmt = conRef.prepareStatement(
                "INSERT INTO " + TBL_KVS +
                    " (" + StringUtils.join(", ", CLM_KVS_NAME, CLM_KVS_UUID, CLM_KVS_DSP_NAME) +
                    ") VALUES (?, ?, ?)"
            );
            PreparedStatement selectKvsStmt = conRef.prepareStatement(
                "SELECT " + CLM_KVS_NAME + " FROM " + TBL_KVS
            );
        )
        {
            HashSet<String> kvsFromProps = new HashSet<>();
            ResultSet propsResultSet = selectPropsStmt.executeQuery();
            while (propsResultSet.next())
            {
                String instanceName = propsResultSet.getString(CLM_PROPS_INSTANCE);
                if (instanceName.startsWith(PROP_INSTANCE_PREFIX))
                {
                    kvsFromProps.add(instanceName.substring(PROP_INSTANCE_PREFIX.length()));
                }
            }

            ResultSet kvsResultSet = selectKvsStmt.executeQuery();
            while (kvsResultSet.next())
            {
                kvsFromProps.remove(kvsResultSet.getString(CLM_KVS_NAME));
            }

            for (String kvsToRestore : kvsFromProps)
            {
                insertKvsStmt.setString(1, kvsToRestore);
                insertKvsStmt.setString(2, UUID.randomUUID().toString());
                insertKvsStmt.setString(3, kvsToRestore.toLowerCase());

                insertKvsStmt.executeUpdate();
            }
        }
    }
}
