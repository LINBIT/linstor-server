package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.02.07.09.30", description = "Remove ON DELETE CASCADE constraints"
)
public class Migration_2020_02_07_RemoveDeleteCascade extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_LUKS_VOLUMES",
            "FK_LCSV_LRI",
            "LAYER_RESOURCE_IDS",
            Collections.singletonList("LAYER_RESOURCE_ID"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "NODE_NET_INTERFACES",
            "FK_NNI_NODES",
            "NODES",
            Collections.singletonList("NODE_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "NODE_STOR_POOL",
            "FK_SP_STOR_POOL_DFNS",
            "STOR_POOL_DEFINITIONS",
            Collections.singletonList("POOL_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "SEC_ID_ROLE_MAP",
            "FK_SIRM_SEC_ROLE",
            "SEC_ROLES",
            Collections.singletonList("ROLE_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_DRBD_VOLUMES",
            "FK_LDV_NSP",
            "NODE_STOR_POOL",
            Arrays.asList("NODE_NAME", "POOL_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_STORAGE_VOLUMES",
            "FK_LSV_SP",
            "NODE_STOR_POOL",
            Arrays.asList("NODE_NAME", "STOR_POOL_NAME"),
            Arrays.asList("NODE_NAME", "POOL_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "SEC_TYPE_RULES",
            "FK_STR_SEC_TYPE_DOMAIN",
            "SEC_TYPES",
            Collections.singletonList("DOMAIN_NAME"),
            Collections.singletonList("TYPE_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "VOLUME_GROUPS",
            "FK_VG_RT",
            "RESOURCE_GROUPS",
            Collections.singletonList("RESOURCE_GROUP_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "NODE_CONNECTIONS",
            "FK_NC_NODES_SRC",
            "NODES",
            Collections.singletonList("NODE_NAME_SRC"),
            Collections.singletonList("NODE_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "NODE_STOR_POOL",
            "FK_SP_NODES",
            "NODES",
            Collections.singletonList("NODE_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_WRITECACHE_VOLUMES",
            "FK_LWV_LRI",
            "LAYER_RESOURCE_IDS",
            Collections.singletonList("LAYER_RESOURCE_ID"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_DRBD_VOLUMES",
            "FK_LDV_LRI",
            "LAYER_RESOURCE_IDS",
            Collections.singletonList("LAYER_RESOURCE_ID"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "SEC_ACL_MAP",
            "FK_SAM_SEC_OBJ_PROT",
            "SEC_OBJECT_PROTECTION",
            Collections.singletonList("OBJECT_PATH"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_DRBD_RESOURCES",
            "FK_LDR_LRI",
            "LAYER_RESOURCE_IDS",
            Collections.singletonList("LAYER_RESOURCE_ID"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "NODE_CONNECTIONS",
            "FK_NC_NODES_DST",
            "NODES",
            Collections.singletonList("NODE_NAME_DST"),
            Collections.singletonList("NODE_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_STORAGE_VOLUMES",
            "FK_LSV_LRI",
            "LAYER_RESOURCE_IDS",
            Collections.singletonList("LAYER_RESOURCE_ID"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "SEC_ID_ROLE_MAP",
            "FK_SIRM_SEC_ID",
            "SEC_IDENTITIES",
            Collections.singletonList("IDENTITY_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "SEC_TYPE_RULES",
            "FK_STR_SEC_TYPE_TYPE",
            "SEC_TYPES",
            Collections.singletonList("TYPE_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "LAYER_WRITECACHE_VOLUMES",
            "FK_LWV_NSP",
            "NODE_STOR_POOL",
            Arrays.asList("NODE_NAME", "POOL_NAME"));

        removeCascadeConstraint(
            connection,
            dbProduct,
            "SEC_DFLT_ROLES",
            "FK_SDR_SEC_ID_ROLE_MAP",
            "SEC_ID_ROLE_MAP",
            Arrays.asList("IDENTITY_NAME", "ROLE_NAME"));

        connection.commit();
    }

    private void removeCascadeConstraint(
        Connection con,
        DbProduct dbProduct,
        final String table,
        final String fkName,
        final String fkTable,
        final List<String> fkColumns,
        final List<String> refColumns) throws SQLException
    {
        SQLUtils.runSql(
            con,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                table,
                fkName
            )
        );

        final String fkColumnsStr = String.join(",", fkColumns);
        final String refColumnsStr = String.join(",", refColumns);
        SQLUtils.runSql(
            con,
            String.format("ALTER TABLE %s ADD CONSTRAINT %s " +
                "FOREIGN KEY (%s) REFERENCES %s(%s)", table, fkName, fkColumnsStr, fkTable, refColumnsStr)
        );
    }

    private void removeCascadeConstraint(
        Connection con,
        DbProduct dbProduct,
        final String table,
        final String fkName,
        final String fkTable,
        final List<String> fkColumns) throws SQLException
    {
        removeCascadeConstraint(
            con,
            dbProduct,
            table,
            fkName,
            fkTable,
            fkColumns,
            fkColumns
        );
    }
}
