package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.SQLException;

@SuppressWarnings({"checkstyle:typename", "checkstyle:magicnumber"})
@Migration(
    version = "2019.03.15.07.00",
    description = "Fix lost database constraints"
)
public class Migration_2019_03_15_FixConstraints extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "VOLUME_DEFINITIONS",
                "FK_VD_RSC_DFN"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "RESOURCES",
                "FK_R_RSC_DFNS"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "SNAPSHOT_VOLUMES",
                "FK_SV_SNAPSHOTS"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "VOLUMES",
                "FK_V_VLM_DFNS"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "LAYER_DRBD_VOLUME_DEFINITIONS",
                "FK_LDRD_VD"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "LAYER_SWORDFISH_VOLUME_DEFINITIONS",
                "FK_LSFVD_VD"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "RESOURCE_CONNECTIONS",
                "FK_RC_RSCS_SRC"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "RESOURCE_CONNECTIONS",
                "FK_RC_RSCS_DST"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "LAYER_RESOURCE_IDS",
                "FK_LRI_RESOURCES"
            )
        );
        attemptSql(
            connection,
            MigrationUtils.dropForeignKeyConstraint(
                dbProduct,
                "VOLUMES",
                "FK_V_RSCS"
            )
        );

        SQLUtils.runSql(
            connection,
            "ALTER TABLE VOLUME_DEFINITIONS ADD CONSTRAINT FK_VD_RSC_DFN " +
            "FOREIGN KEY (RESOURCE_NAME) REFERENCES RESOURCE_DEFINITIONS(RESOURCE_NAME) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE RESOURCES ADD CONSTRAINT FK_R_RSC_DFNS " +
            "FOREIGN KEY (RESOURCE_NAME) REFERENCES RESOURCE_DEFINITIONS(RESOURCE_NAME) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE SNAPSHOT_VOLUMES ADD CONSTRAINT FK_SV_SNAPSHOTS " +
            "FOREIGN KEY (NODE_NAME, RESOURCE_NAME, SNAPSHOT_NAME) REFERENCES " +
            "SNAPSHOTS(NODE_NAME, RESOURCE_NAME, SNAPSHOT_NAME)"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE VOLUMES ADD CONSTRAINT FK_V_VLM_DFNS " +
            "FOREIGN KEY (RESOURCE_NAME, VLM_NR) REFERENCES VOLUME_DEFINITIONS(RESOURCE_NAME, VLM_NR) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE LAYER_DRBD_VOLUME_DEFINITIONS ADD CONSTRAINT FK_LDRD_VD " +
            "FOREIGN KEY (RESOURCE_NAME, VLM_NR) REFERENCES VOLUME_DEFINITIONS(RESOURCE_NAME, VLM_NR) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE LAYER_SWORDFISH_VOLUME_DEFINITIONS ADD CONSTRAINT FK_LSFVD_VD " +
            "FOREIGN KEY (RESOURCE_NAME, VLM_NR) REFERENCES VOLUME_DEFINITIONS(RESOURCE_NAME, VLM_NR) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE RESOURCE_CONNECTIONS ADD CONSTRAINT FK_RC_RSCS_SRC " +
            "FOREIGN KEY (NODE_NAME_SRC, RESOURCE_NAME) REFERENCES RESOURCES(NODE_NAME, RESOURCE_NAME) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE RESOURCE_CONNECTIONS ADD CONSTRAINT FK_RC_RSCS_DST " +
            "FOREIGN KEY (NODE_NAME_DST, RESOURCE_NAME) REFERENCES RESOURCES(NODE_NAME, RESOURCE_NAME) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE LAYER_RESOURCE_IDS ADD CONSTRAINT FK_LRI_RESOURCES " +
            "FOREIGN KEY (NODE_NAME, RESOURCE_NAME) REFERENCES RESOURCES(NODE_NAME, RESOURCE_NAME) " +
            "ON DELETE CASCADE;"
        );
        SQLUtils.runSql(
            connection,
            "ALTER TABLE VOLUMES ADD CONSTRAINT FK_V_RSCS " +
            "FOREIGN KEY (NODE_NAME, RESOURCE_NAME) REFERENCES RESOURCES(NODE_NAME, RESOURCE_NAME) " +
            "ON DELETE CASCADE;"
        );
        connection.commit();
    }

    private void attemptSql(final Connection conn, String statement)
    {
        boolean committed = false;
        try
        {
            SQLUtils.runSql(conn, statement);
            conn.commit();
            committed = true;
        }
        catch (SQLException ignored)
        {
        }
        if (!committed)
        {
            try
            {
                conn.rollback();
            }
            catch (SQLException ignored)
            {
            }
        }
    }
}

