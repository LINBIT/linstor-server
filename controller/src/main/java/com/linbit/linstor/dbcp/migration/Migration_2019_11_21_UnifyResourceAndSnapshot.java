package com.linbit.linstor.dbcp.migration;

import com.linbit.ImplementationError;
import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.utils.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.11.21.01.01",
    description = "Unifies resources and snapshots"
)
/**
 * Fixes the resource definition external name entries
 */
public class Migration_2019_11_21_UnifyResourceAndSnapshot extends LinstorMigration
{

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String PK_RD = "PK_RD";
    private static final String PK_R = "PK_R";
    private static final String PK_RC = "PK_RC";
    private static final String PK_VD = "PK_VD";
    private static final String PK_V = "PK_V";
    private static final String PK_VC = "PK_VC";
    private static final String PK_LDRD = "PK_LDRD";
    private static final String PK_LDVD = "PK_LDVD";
    private static final String PK_LSFVD_OLD = "PK_LSVD";
    private static final String PK_LSFVD_FIXED = "PK_LSFVD";

    private static final String FK_R_RD = "FK_R_RSC_DFNS";
    private static final String FK_R_NODES = "FK_R_NODES";
    private static final String FK_RC_R_SRC = "FK_RC_RSCS_SRC";
    private static final String FK_RC_R_DST = "FK_RC_RSCS_DST";
    private static final String FK_VD_RD = "FK_VD_RSC_DFN";
    private static final String FK_V_R = "FK_V_RSCS";
    private static final String FK_V_VD = "FK_V_VLM_DFNS";
    private static final String FK_VC_V_SRC = "FK_VC_VLMS_SRC";
    private static final String FK_VC_V_DST = "FK_VC_VLMS_DST";
    private static final String FK_SD_RD = "FK_SD_RD";
    private static final String FK_LRI_R_OLD = "FK_LRI_RESOURCES";
    private static final String FK_LRI_R_FIXED = "FK_LRI_R";
    private static final String FK_LDRD_RD = "FK_LDRD_RD";
    private static final String FK_LDVD_VD = "FK_LDVD_VD";
    private static final String FK_LSFVD_VD = "FK_LSFVD_VD";

    private static final String CHK_RD_DSP_NAME = "CHK_RD_DSP_NAME";

    private static final String UNQ_LDVD_MINOR = "UNQ_LDVD_MINOR";

    private static final String TBL_NODES = "NODES";
    private static final String TBL_RSC_DFN = "RESOURCE_DEFINITIONS";
    private static final String TBL_RSC = "RESOURCES";
    private static final String TBL_RSC_CON = "RESOURCE_CONNECTIONS";
    private static final String TBL_VLM_DFN = "VOLUME_DEFINITIONS";
    private static final String TBL_VLM = "VOLUMES";
    private static final String TBL_VLM_CON = "VOLUME_CONNECTIONS";
    private static final String TBL_LAYER_DRBD_RD = "LAYER_DRBD_RESOURCE_DEFINITIONS";
    private static final String TBL_LAYER_DRBD_R = "LAYER_DRBD_RESOURCES";
    private static final String TBL_LAYER_DRBD_VD = "LAYER_DRBD_VOLUME_DEFINITIONS";
    private static final String TBL_LAYER_DRBD_V = "LAYER_DRBD_VOLUMES";
    private static final String TBL_LAYER_IDS = "LAYER_RESOURCE_IDS";
    private static final String TBL_LAYER_SF_VD = "LAYER_SWORDFISH_VOLUME_DEFINITIONS";
    private static final String TBL_LAYER_LUKS_V = "LAYER_LUKS_VOLUMES";
    private static final String TBL_LAYER_S_V = "LAYER_STORAGE_VOLUMES";
    private static final String TBL_SNAP_DFN = "SNAPSHOT_DEFINITIONS";
    private static final String TBL_SNAP = "SNAPSHOTS";
    private static final String TBL_SNAP_VLM_DFN = "SNAPSHOT_VOLUME_DEFINITIONS";
    private static final String TBL_SNAP_VLM = "SNAPSHOT_VOLUMES";
    private static final String TBL_STOR_POOL = "NODE_STOR_POOL";
    private static final String TBL_OBJ_PROT = "SEC_OBJECT_PROTECTION";
    private static final String TBL_ACL = "SEC_ACL_MAP";

    private static final String CLM_UUID = "UUID";
    private static final String CLM_PARENT_UUID = "PARENT_UUID";
    private static final String CLM_NODE_NAME = "NODE_NAME";
    private static final String CLM_NODE_NAME_SRC = "NODE_NAME_SRC";
    private static final String CLM_NODE_NAME_DST = "NODE_NAME_DST";
    private static final String CLM_RSC_NAME = "RESOURCE_NAME";
    private static final String CLM_RSC_DSP_NAME = "RESOURCE_DSP_NAME";
    private static final String CLM_RSC_EXT_NAME = "RESOURCE_EXTERNAL_NAME";
    private static final String CLM_RSC_GRP_NAME = "RESOURCE_GROUP_NAME";
    private static final String CLM_RSC_NAME_SUFFIX = "RESOURCE_NAME_SUFFIX";
    private static final String CLM_RSC_FLAGS = "RESOURCE_FLAGS";
    private static final String CLM_VLM_NR = "VLM_NR";
    private static final String CLM_VLM_SIZE = "VLM_SIZE";
    private static final String CLM_VLM_FLAGS = "VLM_FLAGS";
    private static final String CLM_SNAP_NAME = "SNAPSHOT_NAME";
    private static final String CLM_SNAP_DSP_NAME = "SNAPSHOT_DSP_NAME";
    private static final String CLM_SNAP_FLAGS = "SNAPSHOT_FLAGS";
    private static final String CLM_LAYER_STACK = "LAYER_STACK";
    private static final String CLM_NODE_ID = "NODE_ID";
    private static final String CLM_STOR_POOL_NAME = "STOR_POOL_NAME";
    private static final String CLM_LAYER_RSC_ID = "LAYER_RESOURCE_ID";
    private static final String CLM_LAYER_RSC_NAME_SUFFIX = "LAYER_RESOURCE_SUFFIX";
    private static final String CLM_LAYER_KIND = "LAYER_RESOURCE_KIND";
    private static final String CLM_LAYER_PARENT_ID = "LAYER_RESOURCE_PARENT_ID";
    private static final String CLM_PEER_SLOTS = "PEER_SLOTS";
    private static final String CLM_AL_STRIPES = "AL_STRIPES";
    private static final String CLM_AL_STRIPE_SIZE = "AL_STRIPE_SIZE";
    private static final String CLM_FLAGS = "FLAGS";
    private static final String CLM_TCP_PORT = "TCP_PORT";
    private static final String CLM_TRANSPORT_TYPE = "TRANSPORT_TYPE";
    private static final String CLM_SECRET = "SECRET";
    private static final String CLM_VLM_MINOR_NR = "VLM_MINOR_NR";
    private static final String CLM_POOL_NAME = "POOL_NAME";
    private static final String CLM_ENCRYPTED_PASSWORD = "ENCRYPTED_PASSWORD";
    private static final String CLM_PROVIDER_KIND = "PROVIDER_KIND";
    private static final String CLM_VLM_ODATA = "VLM_ODATA";
    private static final String CLM_DRIVER_NAME = "DRIVER_NAME";
    private static final String CLM_OBJ_PATH = "OBJECT_PATH";
    private static final String CLM_CREATOR_ID_NAME = "CREATOR_IDENTITY_NAME";
    private static final String CLM_OWNER_ROLE_NAME = "OWNER_ROLE_NAME";
    private static final String CLM_SEC_TYPE_NAME = "SECURITY_TYPE_NAME";
    private static final String CLM_ROLE_NAME = "ROLE_NAME";
    private static final String CLM_ACCESS_TYPE = "ACCESS_TYPE";

    private static final String VARCHAR_48 = "VARCHAR(48)";
    private static final String VARCHAR_20 = "VARCHAR(20)";
    private static final String CHAR_36 = "CHARACTER(36)";
    private static final String INTEGER = "INTEGER";

    private static final String LAYER_KIND_DRBD = "DRBD";
    private static final String LAYER_KIND_LUKS = "LUKS";
    private static final String LAYER_KIND_NVME = "NVME";
    private static final String LAYER_KIND_STORAGE = "STORAGE";

    private static final String DFLT_SNAP_NAME = ""; // intentionally empty
    /**
     * RESOURCE_NAME_SUFFIX cannot be anything else for snapshots during this migration
     */
    private static final String DFLT_RSC_NAME_SUFFIX = ""; // intentionally empty
    private static final String DFLT_TRANSPORT_TYPE = "IP";

    // temporary containers to avoid duplicate DB entries (which would cause exceptions)
    private final HashSet<Key> layerDrbdRscDfnSet = new HashSet<>();
    private final HashSet<Key> layerDrbdVlmDfnSet = new HashSet<>();
    private final HashMap<Key, Integer> layerDrbdRscMap = new HashMap<>();
    private final HashMap<Key, Integer> layerRscIdMap = new HashMap<>();

    private int nextLRI = -1;

    @Override
    protected void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        /*
         * currently we have the following tables that have resource name as (partial) primary key:
         *
         * LAYER_DRBD_RESOURCE_DEFINITIONS:
         * PK_LDRD (RESOURCE_NAME, RESOURCE_NAME_SUFFIX)
         * FK_LDRD_RD (RESOURCE_NAME)
         *
         * LAYER_DRBD_VOLUME_DEFINITIONS:
         * PK_LDVD (RESOURCE_NAME, RESOURCE_NAME_SUFFIX, VLM_NR)
         * FK_LDVD_VD (RESOURCE_NAME, VLM_NR)
         * FK_LDRD_VD (RESOURCE_NAME, VLM_NR)
         *
         * LAYER_RESOURCE_IDS:
         * PK_LRI (LAYER_RESOURCE_ID)
         * FK_LRI_RESOURCES (NODE_NAME, RESOURCE_NAME)
         *
         * LAYER_SWORDFISH_VOLUME_DEFINITIONS:
         * PK_LSVD (RESOURCE_NAME, RESOURCE_NAME_SUFFIX, VLM_NR)
         * FK_LSFVD_VD (RESOURCE_NAME, VLM_NR)
         *
         * RESOURCES:
         * PK_R (NODE_NAME, RESOURCE_NAME)
         * FK_R_NODES (NODE_NAME)
         * FK_R_RSC_DFNS (RESOURCE_NAME)
         *
         * RESOURCE_CONNECTIONS:
         * PK_RC (NODE_NAME_SRC, NODE_NAME_DST, RESOURCE_NAME)
         * FK_RC_RSCS_DST (NODE_NAME_DST, RESOURCE_NAME)
         * FK_RC_RSCS_SRC (NODE_NAME_SRC, RESOURCE_NAME)
         *
         * RESOURCE_DEFINITIONS:
         * PK_RD (RESOURCE_NAME)
         *
         * VOLUMES:
         * PK_V (NODE_NAME, RESOURCE_NAME, VLM_NR)
         * FK_V_RSCS (NODE_NAME, RESOURCE_NAME)
         * FK_V_VLM_DFNS (RESOURCE_NAME, VLM_NR)
         *
         * VOLUME_CONNECTIONS:
         * PK_VC (NODE_NAME_SRC, NODE_NAME_DST, RESOURCE_NAME, VLM_NR)
         * FK_VC_VLMS_DST (NODE_NAME_DST, RESOURCE_NAME, VLM_NR)
         * FK_VC_VLMS_SRC (NODE_NAME_SRC, RESOURCE_NAME, VLM_NR)
         *
         * VOLUME_DEFINITIONS:
         * PK_VD (RESOURCE_NAME, VLM_NR)
         * FK_VD_RSC_DFN (RESOURCE_NAME)
         *
         * All tables will be extended with SNAPSHOT_NAME, defaulting to '' and becoming part of the
         * primary key for each table. (not necessarily in this order as we have to take care in which
         * order we create the new PRIMARY and FOREIGN KEYs)
         *
         * RESOURCE_DEFINITION will additionally be extended with SNAPSHOT_DSP_NAME
         *
         * RESOUCE_CONNECTIONS and VOLUME_CONNECTIONS will be extended as stated above, but will not
         * receive any data migration as we simply did not save those anywhere
         * until now (i.e. there is no source where we could migrate data from)
         *
         * SNAPSHOT_VOLUMES will be merged into VOLUMES
         * SNAPSHOT_RESOURCES will be merged into RESOURCES
         * SNAPSHOT_VOLUME_DEFINITIONS will be merged into VOLUME_DEFINITIONS
         * SNAPSHOT_RESOURCE_DEFINITIONS will be merged into RESOURCE_DEFINITIONS
         */

        dropAllRelatedForeignKeys(dbCon, dbProduct);

        alterResourceDefinitions(dbCon, dbProduct);
        alterResources(dbCon, dbProduct);
        alterResourceConnections(dbCon, dbProduct);

        alterVolumeDefinitions(dbCon, dbProduct);
        alterVolumes(dbCon, dbProduct);
        alterVolumeConnections(dbCon, dbProduct);

        alterLayerIds(dbCon, dbProduct);
        alterLayerDrbdResourceDefinitions(dbCon, dbProduct);
        alterLayerDrbdVolumeDefinitions(dbCon, dbProduct);
        alterLayerSwordfishVolumeDefinitions(dbCon, dbProduct);

        // snapshot columns added, primary and foreign keys updated

        copySnapshotDefinitionsIntoResourceDefinitions(dbCon);
        copySnapshotVolumeDefinitionsIntoVolumeDefinitions(dbCon);
        copySnapshotsIntoResources(dbCon);
        copySnapshotVolumesIntoVolumes(dbCon);

        createObjectProtectionForSnapshotDefinitions(dbCon);

        createLayerDataForSnapshots(dbCon);

        dropTable(dbCon, TBL_SNAP_VLM);
        dropTable(dbCon, TBL_SNAP_VLM_DFN);
        dropTable(dbCon, TBL_SNAP);
        dropTable(dbCon, TBL_SNAP_DFN);
    }

    /**
     * Databases should usually allow dropping a primary key, even if foreign keys refer to it.
     * However, at least H2 does not support that.
     *
     * @param dbConRef
     * @param dbProductRef
     * @throws SQLException
     */
    private void dropAllRelatedForeignKeys(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        // following FKs will be recreated later
        dropForeignKey(dbCon, dbProduct, TBL_LAYER_DRBD_RD, FK_LDRD_RD);
        dropForeignKey(dbCon, dbProduct, TBL_LAYER_DRBD_VD, FK_LDVD_VD);
        dropForeignKey(dbCon, dbProduct, TBL_LAYER_IDS, FK_LRI_R_OLD);
        dropForeignKey(dbCon, dbProduct, TBL_LAYER_SF_VD, FK_LSFVD_VD);
        dropForeignKey(dbCon, dbProduct, TBL_RSC_CON, FK_RC_R_SRC);
        dropForeignKey(dbCon, dbProduct, TBL_RSC_CON, FK_RC_R_DST);
        dropForeignKey(dbCon, dbProduct, TBL_RSC, FK_R_RD);
        dropForeignKey(dbCon, dbProduct, TBL_RSC, FK_R_NODES);
        dropForeignKey(dbCon, dbProduct, TBL_VLM_CON, FK_VC_V_DST);
        dropForeignKey(dbCon, dbProduct, TBL_VLM_CON, FK_VC_V_SRC);
        dropForeignKey(dbCon, dbProduct, TBL_VLM_DFN, FK_VD_RD);
        dropForeignKey(dbCon, dbProduct, TBL_VLM, FK_V_R);
        dropForeignKey(dbCon, dbProduct, TBL_VLM, FK_V_VD);

        // following FKs will not be created, but the whole table will be dropped
        dropForeignKey(dbCon, dbProduct, TBL_SNAP_DFN, "FK_SD_RSC_DFN");

        // following FKs will not be recreated as the have wrong abbreviation (the correctly named FKs will be
        // recreated)
        dropForeignKey(dbCon, dbProduct, TBL_LAYER_DRBD_VD, "FK_LDRD_VD");
    }

    private void dropForeignKey(Connection dbCon, DbProduct dbProduct, String table, String fkName)
        throws SQLException
    {
        SQLUtils.runSql(
            dbCon,
            MigrationUtils.dropColumnConstraintForeignKey(dbProduct, table, fkName)
        );
    }

    private void alterResourceDefinitions(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * RESOURCE_DEFINITIONS:
         * PK_RD (RESOURCE_NAME)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_RSC_DFN, CLM_RSC_NAME);
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumn(
                dbProduct, TBL_RSC_DFN, CLM_SNAP_DSP_NAME, VARCHAR_48, false, DFLT_SNAP_NAME, CLM_RSC_DSP_NAME
            )
        );
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumn(dbProduct, TBL_RSC_DFN, CLM_PARENT_UUID, CHAR_36, true, null, null)
        );
        // make RSC_DSP_NAME nullable
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.dropColumnConstraintNotNull(
                dbProduct,
                TBL_RSC_DFN,
                CLM_RSC_DSP_NAME,
                VARCHAR_48
            )
        );

        // add checks
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintCheck(
                dbProduct,
                TBL_RSC_DFN,
                "CHK_RD_SNAP_NAME",
                String.format("(UPPER(%1$s) = %1$s AND LENGTH(%1$s) <> 1)", CLM_SNAP_NAME)
            )
        );
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintCheck(
                dbProduct,
                TBL_RSC_DFN,
                "CHK_RD_SNAP_DSP_NAME",
                String.format("(UPPER(%s) = %s)", CLM_SNAP_DSP_NAME, CLM_SNAP_NAME)
            )
        );
        // recreate RSC_DSP_NAME check
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.dropColumnConstraintCheck(dbCon, dbProduct, TBL_RSC_DFN, CHK_RD_DSP_NAME)
        );
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintCheck(
                dbProduct,
                TBL_RSC_DFN,
                CHK_RD_DSP_NAME,
                String.format(
                    "(%2$s IS NOT NULL AND UPPER(%2$s) = %1$s) OR " +
                        "(%2$s IS NULL AND LENGTH(%3$s) > 1)",
                    CLM_RSC_NAME,
                    CLM_RSC_DSP_NAME,
                    CLM_SNAP_NAME
                )
            )
        );

        recreatePrimaryKey(dbCon, dbProduct, TBL_RSC_DFN, PK_RD, CLM_RSC_NAME, CLM_SNAP_NAME);
        // recreate foreign key from SNAPSHOT_DEFINITIONS to RESOURCE_DEFINITIONS
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintForeignKey(
                dbProduct,
                TBL_RSC_DFN,
                FK_SD_RD,
                CLM_PARENT_UUID,
                TBL_RSC_DFN, // self reference
                CLM_UUID
            )
        );
    }

    private void alterResources(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * RESOURCES:
         * PK_R (NODE_NAME, RESOURCE_NAME)
         * FK_R_NODES (NODE_NAME)
         * FK_R_RSC_DFNS (RESOURCE_NAME)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_RSC, CLM_RSC_NAME);
        recreatePrimaryKey(dbCon, dbProduct, TBL_RSC, PK_R, CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME);
        recreateForeignKey(dbCon, dbProduct, TBL_RSC, FK_R_NODES, TBL_NODES, CLM_NODE_NAME);
        recreateForeignKey(dbCon, dbProduct, TBL_RSC, FK_R_RD, TBL_RSC_DFN, CLM_RSC_NAME, CLM_SNAP_NAME);
    }

    private void alterResourceConnections(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * RESOURCE_CONNECTIONS:
         * PK_RC (NODE_NAME_SRC, NODE_NAME_DST, RESOURCE_NAME)
         * FK_RC_RSCS_DST (NODE_NAME_DST, RESOURCE_NAME)
         * FK_RC_RSCS_SRC (NODE_NAME_SRC, RESOURCE_NAME)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_RSC_CON, CLM_RSC_NAME);
        recreatePrimaryKey(
            dbCon,
            dbProduct,
            TBL_RSC_CON,
            PK_RC,
            CLM_NODE_NAME_SRC, CLM_NODE_NAME_DST, CLM_RSC_NAME, CLM_SNAP_NAME
        );
        // not using recreateFK method as we have different column names in local table as in remote table
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintForeignKey(
                dbProduct,
                TBL_RSC_CON,
                FK_RC_R_SRC,
                join(CLM_NODE_NAME_SRC, CLM_RSC_NAME, CLM_SNAP_NAME),
                TBL_RSC,
                join(CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME)
            )
        );
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintForeignKey(
                dbProduct,
                TBL_RSC_CON,
                FK_RC_R_DST,
                join(CLM_NODE_NAME_DST, CLM_RSC_NAME, CLM_SNAP_NAME),
                TBL_RSC,
                join(CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME)
            )
        );
    }

    private void alterVolumeDefinitions(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * VOLUME_DEFINITIONS:
         * PK_VD (RESOURCE_NAME, VLM_NR)
         * FK_VD_RSC_DFN (RESOURCE_NAME)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_VLM_DFN, CLM_RSC_NAME);
        recreatePrimaryKey(dbCon, dbProduct, TBL_VLM_DFN, PK_VD, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR);
        recreateForeignKey(dbCon, dbProduct, TBL_VLM_DFN, FK_VD_RD, TBL_RSC_DFN, CLM_RSC_NAME, CLM_SNAP_NAME);
    }

    private void alterVolumes(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * VOLUMES:
         * PK_V (NODE_NAME, RESOURCE_NAME, VLM_NR)
         * FK_V_RSCS (NODE_NAME, RESOURCE_NAME)
         * FK_V_VLM_DFNS (RESOURCE_NAME, VLM_NR)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_VLM, CLM_RSC_NAME);
        recreatePrimaryKey(dbCon, dbProduct, TBL_VLM, PK_V, CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR);
        recreateForeignKey(dbCon, dbProduct, TBL_VLM, FK_V_R, TBL_RSC, CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME);
        recreateForeignKey(dbCon, dbProduct, TBL_VLM, FK_V_VD, TBL_VLM_DFN, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR);
    }

    private void alterVolumeConnections(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * VOLUME_CONNECTIONS:
         * PK_VC (NODE_NAME_SRC, NODE_NAME_DST, RESOURCE_NAME, VLM_NR)
         * FK_VC_VLMS_DST (NODE_NAME_DST, RESOURCE_NAME, VLM_NR)
         * FK_VC_VLMS_SRC (NODE_NAME_SRC, RESOURCE_NAME, VLM_NR)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_VLM_CON, CLM_RSC_NAME);
        recreatePrimaryKey(
            dbCon,
            dbProduct,
            TBL_VLM_CON,
            PK_VC,
            CLM_NODE_NAME_SRC, CLM_NODE_NAME_DST, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR
        );
        // not using recreateFK method as column names of local and remote tables differ
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintForeignKey(
                dbProduct,
                TBL_VLM_CON,
                FK_VC_V_DST,
                join(CLM_NODE_NAME_DST, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR),
                TBL_VLM,
                join(CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR)
            )
        );
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintForeignKey(
                dbProduct,
                TBL_VLM_CON,
                FK_VC_V_SRC,
                join(CLM_NODE_NAME_SRC, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR),
                TBL_VLM,
                join(CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR)
            )
        );
    }

    private void alterLayerIds(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * LAYER_RESOURCE_IDS:
         * PK_LRI (LAYER_RESOURCE_ID)
         * FK_LRI_RESOURCES (NODE_NAME, RESOURCE_NAME)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_LAYER_IDS, CLM_RSC_NAME);
        // PK does not need to be updated
        recreateForeignKey(
            dbCon, dbProduct, TBL_LAYER_IDS, FK_LRI_R_FIXED, TBL_RSC, CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME
        );
    }

    private void alterLayerDrbdResourceDefinitions(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * LAYER_DRBD_RESOURCE_DEFINITIONS:
         * PK_LDRD (RESOURCE_NAME, RESOURCE_NAME_SUFFIX)
         * FK_LDRD_RD (RESOURCE_NAME)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_LAYER_DRBD_RD, CLM_RSC_NAME_SUFFIX);
        recreatePrimaryKey(
            dbCon, dbProduct, TBL_LAYER_DRBD_RD, PK_LDRD, CLM_RSC_NAME, CLM_RSC_NAME_SUFFIX, CLM_SNAP_NAME
        );
        recreateForeignKey(dbCon, dbProduct, TBL_LAYER_DRBD_RD, FK_LDRD_RD, TBL_RSC_DFN, CLM_RSC_NAME, CLM_SNAP_NAME);

        // snapshots will not want to store TCP_PORT
        SQLUtils.runSql(
            dbCon,
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_LAYER_DRBD_RD, CLM_TCP_PORT, INTEGER)
        );
        // snapshots will not want to store SECRET
        SQLUtils.runSql(
            dbCon,
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_LAYER_DRBD_RD, CLM_SECRET, VARCHAR_20)
        );
    }

    private void alterLayerDrbdVolumeDefinitions(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * LAYER_DRBD_VOLUME_DEFINITIONS:
         * PK_LDVD (RESOURCE_NAME, RESOURCE_NAME_SUFFIX, VLM_NR)
         * FK_LDVD_VD (RESOURCE_NAME, VLM_NR)
         * FK_LDRD_VD (RESOURCE_NAME, VLM_NR)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_LAYER_DRBD_VD, CLM_RSC_NAME_SUFFIX);
        recreatePrimaryKey(
            dbCon, dbProduct, TBL_LAYER_DRBD_VD, PK_LDVD, CLM_RSC_NAME, CLM_RSC_NAME_SUFFIX, CLM_SNAP_NAME, CLM_VLM_NR
        );
        recreateForeignKey(
            dbCon, dbProduct, TBL_LAYER_DRBD_VD, FK_LDVD_VD, TBL_VLM_DFN, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR
        );
        // snapshots will not want to store VLM_MINOR_NR
        SQLUtils.runSql(
            dbCon,
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_LAYER_DRBD_VD, CLM_VLM_MINOR_NR, INTEGER)
        );
    }

    private void alterLayerSwordfishVolumeDefinitions(Connection dbCon, DbProduct dbProduct) throws SQLException
    {
        /*
         * LAYER_SWORDFISH_VOLUME_DEFINITIONS:
         * PK_LSVD (RESOURCE_NAME, RESOURCE_NAME_SUFFIX, VLM_NR)
         * FK_LSFVD_VD (RESOURCE_NAME, VLM_NR)
         */
        addSnapNameColumn(dbCon, dbProduct, TBL_LAYER_SF_VD, CLM_RSC_NAME);
        // not using recreatePK method as we are also fixing wrong abbreviation
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.dropColumnConstraintPrimaryKey(dbProduct, TBL_LAYER_SF_VD, PK_LSFVD_OLD)
        );
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintPrimaryKey(
                dbProduct,
                TBL_LAYER_SF_VD,
                PK_LSFVD_FIXED,
                CLM_RSC_NAME, CLM_RSC_NAME_SUFFIX, CLM_SNAP_NAME, CLM_VLM_NR
            )
        );
        recreateForeignKey(
            dbCon, dbProduct, TBL_LAYER_SF_VD, FK_LSFVD_VD, TBL_VLM_DFN, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_VLM_NR
        );
    }


    /**
     * Adds the SNAPSHOT_NAME VARCHAR(48) column to the given table
     *
     * @param dbCon
     * @param dbProduct
     * @param table
     * @throws SQLException
     */
    private void addSnapNameColumn(Connection dbCon, DbProduct dbProduct, String table, String afterColumn)
        throws SQLException
    {
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumn(
                dbProduct,
                table,
                CLM_SNAP_NAME,
                VARCHAR_48,
                false,
                DFLT_SNAP_NAME,
                afterColumn
            )
        );
    }

    /**
     * Drops and (re) creates the primary key for the given table and the given primary key name using the vararg
     * argument
     *
     * @param dbCon
     * @param dbProduct
     * @param table
     * @param pkName
     * @param pkClms
     * @throws SQLException
     */
    private void recreatePrimaryKey(
        Connection dbCon,
        DbProduct dbProduct,
        String table,
        String pkName,
        String... pkClms
    )
        throws SQLException
    {
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.dropColumnConstraintPrimaryKey(dbProduct, table, pkName)
        );
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintPrimaryKey(dbProduct, table, pkName, pkClms)
        );
    }

    /**
     * Drops and (re) creates the foreign key for the given localTable and the given foreign key name.
     * This method assumes that the used columns are the same in the localTable as well as in the remoteTable
     *
     * @param dbCon
     * @param dbProduct
     * @param localTable
     * @param fkName
     * @param remoteTable
     * @param fkClms
     * @throws SQLException
     */
    private void recreateForeignKey(
        Connection dbCon,
        DbProduct dbProduct,
        String localTable,
        String fkName,
        String remoteTable,
        String... fkClms
    )
        throws SQLException
    {
        String joinedFkColumns = join(fkClms);
        SQLUtils.executeStatement(
            dbCon,
            MigrationUtils.addColumnConstraintForeignKey(
                dbProduct,
                localTable,
                fkName,
                joinedFkColumns,
                remoteTable,
                joinedFkColumns
            )
        );
    }

    private static String join(String... sqlColumns)
    {
        return StringUtils.join(", ", sqlColumns);
    }

    private void copySnapshotDefinitionsIntoResourceDefinitions(Connection dbCon) throws SQLException
    {
        final String[] newRscDfnColumns = new String[]
        {
            CLM_UUID,
            CLM_RSC_NAME,
            CLM_RSC_DSP_NAME,
            CLM_SNAP_NAME,
            CLM_SNAP_DSP_NAME,
            CLM_RSC_FLAGS,
            CLM_LAYER_STACK,
            CLM_RSC_EXT_NAME,
            CLM_RSC_GRP_NAME,
            CLM_PARENT_UUID
        };

        final String copySQL = "INSERT INTO " + TBL_RSC_DFN +
            "(" + join(newRscDfnColumns) + ") " +
            " SELECT " +
                CLM_UUID + ", " +
                CLM_RSC_NAME + ", " +
                "NULL, " + // no rscDspName
                CLM_SNAP_NAME + ", " +
                CLM_SNAP_DSP_NAME + ", " +
                CLM_SNAP_FLAGS + ", " + // copy into RESOURCE_FLAGS
                "'[]', " + // no LayerStack
                "NULL, " + // no RSESOURCE_EXTERNAL_NAME,
                "'" + InternalApiConsts.DEFAULT_RSC_GRP_NAME.toUpperCase() + "', " +
                "(" +
                "   SELECT " + CLM_UUID +
                "   FROM " + TBL_RSC_DFN + " AS RD " +
                "   WHERE RD." + CLM_RSC_NAME + " = SD." + CLM_RSC_NAME +
                ")" + // new PARENT_UUID
            " FROM " + TBL_SNAP_DFN + " AS SD";


        try (PreparedStatement stmt = dbCon.prepareStatement(copySQL);)
        {
            stmt.executeUpdate();
        }
    }

    private void copySnapshotVolumeDefinitionsIntoVolumeDefinitions(Connection dbCon) throws SQLException
    {
        final String[] newVlmDfnColumns = new String[]
        {
            CLM_UUID,
            CLM_RSC_NAME,
            CLM_SNAP_NAME,
            CLM_VLM_NR,
            CLM_VLM_SIZE,
            CLM_VLM_FLAGS
        };
        final String copySQL = "INSERT INTO " + TBL_VLM_DFN + "(" +
            join(newVlmDfnColumns) + ") " +
            " SELECT " +
                CLM_UUID + ", " +
                CLM_RSC_NAME + ", " +
                CLM_SNAP_NAME + ", " +
                CLM_VLM_NR + ", " +
                CLM_VLM_SIZE + ", " +
                CLM_SNAP_FLAGS +
            " FROM " + TBL_SNAP_VLM_DFN;
        try (PreparedStatement stmt = dbCon.prepareStatement(copySQL))
        {
            stmt.executeUpdate();
        }
    }

    private void copySnapshotsIntoResources(Connection dbCon) throws SQLException
    {
        final String[] newRscColumn = new String[]
        {
            CLM_UUID,
            CLM_NODE_NAME,
            CLM_RSC_NAME,
            CLM_SNAP_NAME,
            CLM_RSC_FLAGS
        };
        final String copySQL = "INSERT INTO " + TBL_RSC +
            "(" + join(newRscColumn) + ") " +
            "SELECT " +
                CLM_UUID + ", " +
                CLM_NODE_NAME + ", " +
                CLM_RSC_NAME + ", " +
                CLM_SNAP_NAME + ", " +
                CLM_SNAP_FLAGS +  // copy into RESOURCE_FLAGS
            " FROM " + TBL_SNAP;
        /*
         * SNAPSHOT.NODE_ID and SNAPSHOT.LAYER_STACK are ignored for now.
         * Those columns will be processed when creating the new layer data
         */
        try (PreparedStatement stmt = dbCon.prepareStatement(copySQL))
        {
            stmt.executeUpdate();
        }
    }

    private void copySnapshotVolumesIntoVolumes(Connection dbCon) throws SQLException
    {
        final String[] newVlmColumn = new String[]
        {
            CLM_UUID,
            CLM_NODE_NAME,
            CLM_RSC_NAME,
            CLM_SNAP_NAME,
            CLM_VLM_NR,
            CLM_VLM_FLAGS
        };
        final String copySQL = "INSERT INTO " + TBL_VLM +
            "(" + join(newVlmColumn) + ") " +
            "SELECT " +
                CLM_UUID + ", " +
                CLM_NODE_NAME + ", " +
                CLM_RSC_NAME + ", " +
                CLM_SNAP_NAME + ", " +
                CLM_VLM_NR + ", " +
                "0" +  // snapshotVolumes have no flags
            " FROM " + TBL_SNAP_VLM;
        /*
         * SNAPSHOT_VOLUMES.STOR_POOL_NAME is ignored for now.
         * That column will be processed when creating the new layer data
         */
        try (PreparedStatement stmt = dbCon.prepareStatement(copySQL))
        {
            stmt.executeUpdate();
        }
    }

    private void createObjectProtectionForSnapshotDefinitions(Connection dbCon)
        throws SQLException
    {
        final String[] objProtColumns = new String[]
        {
            CLM_OBJ_PATH,
            CLM_CREATOR_ID_NAME,
            CLM_OWNER_ROLE_NAME,
            CLM_SEC_TYPE_NAME
        };
        final String[] aclColumns = new String[]
        {
            CLM_OBJ_PATH,
            CLM_ROLE_NAME,
            CLM_ACCESS_TYPE
        };
        final String selectAllSnapDfnsSql = " SELECT " + CLM_RSC_NAME + ", " + CLM_SNAP_NAME +
            " FROM " + TBL_SNAP_DFN;
        final String insertIntoObjProtSql = "INSERT INTO " + TBL_OBJ_PROT + "(" + join(objProtColumns) + ") " +
            " VALUES (?, 'PUBLIC', 'PUBLIC', 'PUBLIC')";
        final String insertIntoAcl = "INSERT INTO " + TBL_ACL + "(" + join(aclColumns) + ") " +
            " VALUES (?, 'PUBLIC', 15)";

        try (
            PreparedStatement select = dbCon.prepareStatement(selectAllSnapDfnsSql);
            PreparedStatement insertObjProt = dbCon.prepareStatement(insertIntoObjProtSql);
            PreparedStatement insertAcl = dbCon.prepareStatement(insertIntoAcl);

            ResultSet result = select.executeQuery();
        )
        {
            while (result.next())
            {
                String rscName = result.getString(CLM_RSC_NAME);
                String snapName = result.getString(CLM_SNAP_NAME);

                String objProtPath = "/snapshotdefinitions/" + rscName + "/" + snapName;
                insertObjProt.setString(1, objProtPath);
                insertObjProt.executeUpdate();

                insertAcl.setString(1, objProtPath);
                insertAcl.executeUpdate();
            }
        }
    }

    private void createLayerDataForSnapshots(Connection dbCon)
        throws SQLException, JsonParseException, JsonMappingException, IOException
    {
        final String selectMaxLRI = "SELECT MAX(" + CLM_LAYER_RSC_ID + ") FROM " + TBL_LAYER_IDS;
        /*
         * Similar to Migration_2019_02_20_LayerData, we will now iterate over all SnapshotVolumes
         * with an inner join to SnapshotResources so that we have all layer specific data stored by
         * the snapshots
         */
        final String selectSnapVlmsJoinedWithSnapshots =
            "SELECT " +
            // "S." + CLM_UUID + " AS S_UUID, " +
            // "SV." + CLM_UUID + " AS SV_UUID, " +
                "S." + CLM_NODE_NAME + ", " +
                "S." + CLM_RSC_NAME + ", " +
                "S." + CLM_SNAP_NAME + ", " +
                "SV." + CLM_VLM_NR + ", " +
                "S." + CLM_NODE_ID + ", " +
                "S." + CLM_LAYER_STACK + ", " +
                "SV." + CLM_STOR_POOL_NAME + ", " +
                "SP." + CLM_DRIVER_NAME +
            " FROM " +
                TBL_SNAP_VLM + " AS SV, " +
                TBL_SNAP + " AS S, " +
                TBL_STOR_POOL + " AS SP " +
            " WHERE " +
                "S." + CLM_NODE_NAME + " = SV." + CLM_NODE_NAME + " AND " +
                "S." + CLM_RSC_NAME + " = SV." + CLM_RSC_NAME + " AND " +
                "S." + CLM_SNAP_NAME + " = SV." + CLM_SNAP_NAME + " AND " +
                "SP." + CLM_NODE_NAME + " = SV." + CLM_NODE_NAME + " AND " +
                "SP." + CLM_POOL_NAME + " = SV." + CLM_STOR_POOL_NAME;
        final String insertLDR = generateInsertStmt(
            TBL_LAYER_DRBD_R,
            CLM_LAYER_RSC_ID, CLM_PEER_SLOTS, CLM_AL_STRIPES, CLM_AL_STRIPE_SIZE, CLM_FLAGS, CLM_NODE_ID
        );
        final String insertLDRD = generateInsertStmt(
            TBL_LAYER_DRBD_RD,
            CLM_RSC_NAME, CLM_RSC_NAME_SUFFIX, CLM_SNAP_NAME, CLM_PEER_SLOTS, CLM_AL_STRIPES,
            CLM_AL_STRIPE_SIZE, CLM_TCP_PORT, CLM_TRANSPORT_TYPE, CLM_SECRET
        );
        final String insertLDV = generateInsertStmt(
            TBL_LAYER_DRBD_V,
            CLM_LAYER_RSC_ID, CLM_VLM_NR, CLM_NODE_NAME, CLM_POOL_NAME
        );
        final String insertLDVD = generateInsertStmt(
            TBL_LAYER_DRBD_VD,
            CLM_RSC_NAME, CLM_RSC_NAME_SUFFIX, CLM_SNAP_NAME, CLM_VLM_NR, CLM_VLM_MINOR_NR
        );
        final String insertLLV = generateInsertStmt(
            TBL_LAYER_LUKS_V,
            CLM_LAYER_RSC_ID, CLM_VLM_NR, CLM_ENCRYPTED_PASSWORD
        );
        final String insertLRI = generateInsertStmt(
            TBL_LAYER_IDS,
            CLM_LAYER_RSC_ID, CLM_NODE_NAME, CLM_RSC_NAME, CLM_SNAP_NAME, CLM_LAYER_KIND, CLM_LAYER_PARENT_ID,
            CLM_LAYER_RSC_NAME_SUFFIX
        );
        final String insertLSV = generateInsertStmt(
            TBL_LAYER_S_V,
            CLM_LAYER_RSC_ID, CLM_VLM_NR, CLM_PROVIDER_KIND, CLM_NODE_NAME, CLM_STOR_POOL_NAME
        );
        final String insertLSFVD = generateInsertStmt(
            TBL_LAYER_SF_VD,
            CLM_RSC_NAME, CLM_RSC_NAME_SUFFIX, CLM_SNAP_NAME, CLM_VLM_NR, CLM_VLM_ODATA
        );

        try (
            PreparedStatement selectMaxLRIStmt = dbCon.prepareStatement(selectMaxLRI);
            ResultSet maxLRIresultSet = selectMaxLRIStmt.executeQuery();
            PreparedStatement selectStmt = dbCon.prepareStatement(selectSnapVlmsJoinedWithSnapshots);
            ResultSet resultSet = selectStmt.executeQuery();
            PreparedStatement insLDR = dbCon.prepareStatement(insertLDR);
            PreparedStatement insLDRD = dbCon.prepareStatement(insertLDRD);
            PreparedStatement insLDV = dbCon.prepareStatement(insertLDV);
            PreparedStatement insLDVD = dbCon.prepareStatement(insertLDVD);
            PreparedStatement insLLV = dbCon.prepareStatement(insertLLV);
            PreparedStatement insLRI = dbCon.prepareStatement(insertLRI);
            PreparedStatement insLSV = dbCon.prepareStatement(insertLSV);
            PreparedStatement insLSFVD = dbCon.prepareStatement(insertLSFVD);
        )
        {
            if (!maxLRIresultSet.next())
            {
                nextLRI = 0;
            }
            else
            {
                nextLRI = maxLRIresultSet.getInt(1) + 1;
            }

            while (resultSet.next())
            {
                String nodeName = resultSet.getString(CLM_NODE_NAME);
                String rscName = resultSet.getString(CLM_RSC_NAME);
                String snapName = resultSet.getString(CLM_SNAP_NAME);
                int vlmNr = resultSet.getInt(CLM_VLM_NR);
                String storPoolName = resultSet.getString(CLM_STOR_POOL_NAME);
                String storPoolKind = resultSet.getString(CLM_DRIVER_NAME);

                Integer nodeId = resultSet.getInt(CLM_NODE_ID);
                if (resultSet.wasNull())
                {
                    nodeId = null;
                }
                String layerStackStr = resultSet.getString(CLM_LAYER_STACK);

                List<String> layerStack = OBJECT_MAPPER.readValue(layerStackStr, List.class);
                Integer parentId = null;
                for (String layerStr : layerStack)
                {
                    // layerdata will only have single children (no or always empty RESOURCE_NAME_SUFFIX)
                    switch (layerStr)
                    {
                        case LAYER_KIND_DRBD:
                            createLayerDrbdRscDfnEntry(insLDRD, rscName, snapName);
                            createLayerDrbdVlmDfnEntry(insLDVD, rscName, snapName, vlmNr);
                            parentId = createLayerDrbdRscEntry(
                                insLRI, parentId, insLDR, nodeName, rscName, snapName, nodeId
                            );
                            createLayerDrbdVlmEntry(insLDV, parentId, vlmNr);
                            break;
                        case LAYER_KIND_LUKS:
                            // we did not save the password.
                            // we will create the luks layer data, but the snapshot will not be usable
                            parentId = createLayerRscIdEntry(
                                insLRI, nodeName, rscName, snapName, LAYER_KIND_LUKS, parentId
                            );
                            createLayerLuksVlmEntry(insLLV, parentId, vlmNr); // no key :(
                            break;
                        case LAYER_KIND_NVME:
                            parentId = createLayerRscIdEntry(
                                insLRI, nodeName, rscName, snapName, LAYER_KIND_NVME, parentId
                            );
                            break;
                        case LAYER_KIND_STORAGE:
                            parentId = createLayerRscIdEntry(
                                insLRI, nodeName, rscName, snapName, LAYER_KIND_STORAGE, parentId
                            );
                            createLayerStorVlmEntry(insLSV, parentId, vlmNr, storPoolKind, nodeName, storPoolName);
                            break;
                        default:
                            throw new ImplementationError("Unknown layer kind: " + layerStr);
                    }
                }
            }
        }
    }

    private String generateInsertStmt(String table, String... colums)
    {
        return "INSERT INTO " + table +
            "(" + join(colums) +
            ") VALUES (" +
            StringUtils.repeat("?", ", ", colums.length) +
            ")";
    }

    private void createLayerDrbdRscDfnEntry(PreparedStatement insert, String rscName, String snapName)
        throws SQLException
    {
        if (layerDrbdRscDfnSet.add(new Key(rscName, snapName)))
        {
            insert.setString(1, rscName);
            insert.setString(2, DFLT_RSC_NAME_SUFFIX);
            insert.setString(3, snapName);
            insert.setInt(4, InternalApiConsts.DEFAULT_PEER_SLOTS); // we have nothing else saved
            insert.setInt(5, InternalApiConsts.DEFAULT_AL_STRIPES);
            insert.setLong(6, InternalApiConsts.DEFAULT_AL_SIZE);
            insert.setNull(7, Types.INTEGER); // null as tcpPort
            insert.setString(8, DFLT_TRANSPORT_TYPE);
            insert.setNull(9, Types.VARCHAR); // no secret

            insert.executeUpdate();
        }
    }

    private void createLayerDrbdVlmDfnEntry(PreparedStatement insert, String rscName, String snapName, int vlmNr)
        throws SQLException
    {
        if (layerDrbdVlmDfnSet.add(new Key(rscName, snapName, vlmNr)))
        {
            insert.setString(1, rscName);
            insert.setString(2, DFLT_RSC_NAME_SUFFIX);
            insert.setString(3, snapName);
            insert.setInt(4, vlmNr);
            insert.setNull(5, Types.INTEGER); // null minor number
            // insert.setInt(5, DFLT_SNAP_MINOR);

            insert.executeUpdate();
        }
    }

    private int createLayerDrbdRscEntry(
        PreparedStatement insertLRI,
        @Nullable Integer parentId,
        PreparedStatement insertDrbdRsc,
        String nodeName,
        String rscName,
        String snapName,
        Integer nodeId
    )
        throws SQLException
    {
        Key key = new Key(nodeName, rscName, snapName);
        Integer id = layerDrbdRscMap.get(key);
        if (id == null)
        {
            id = createLayerRscIdEntry(insertLRI, nodeName, rscName, snapName, LAYER_KIND_DRBD, parentId);

            insertDrbdRsc.setInt(1, id);
            insertDrbdRsc.setInt(2, InternalApiConsts.DEFAULT_PEER_SLOTS); // we have nothing else saved
            insertDrbdRsc.setInt(3, InternalApiConsts.DEFAULT_AL_STRIPES);
            insertDrbdRsc.setLong(4, InternalApiConsts.DEFAULT_AL_SIZE);
            insertDrbdRsc.setInt(5, 0); // we have no flags saved
            if (nodeId == null)
            {
                // should never be null, but if it still is, at least we should not abort migration.
                insertDrbdRsc.setNull(6, Types.INTEGER);
            }
            else
            {
                insertDrbdRsc.setInt(6, nodeId);
            }
            insertDrbdRsc.executeUpdate();

            layerDrbdRscMap.put(key, id);
        }
        return id;
    }

    private void createLayerDrbdVlmEntry(
        PreparedStatement insert,
        Integer parentId,
        int vlmNr
    )
        throws SQLException
    {
        insert.setInt(1, parentId);
        insert.setInt(2, vlmNr);
        insert.setNull(3, Types.VARCHAR); // != null would mean external metadata
        insert.setNull(4, Types.VARCHAR); // until now, linstor refused to create snapshot from external metadata ->
                                          // cannot happen
        insert.executeUpdate();
    }

    private void createLayerLuksVlmEntry(PreparedStatement insert, Integer parentId, int vlmNr)
        throws SQLException
    {
        insert.setInt(1, parentId);
        insert.setInt(2, vlmNr);
        insert.setBytes(3, new byte[0]);
        insert.executeUpdate();
    }

    private void createLayerStorVlmEntry(
        PreparedStatement insert,
        Integer parentId,
        int vlmNr,
        String storPoolKind,
        String nodeName,
        String storPoolName
    )
        throws SQLException
    {
        insert.setInt(1, parentId);
        insert.setInt(2, vlmNr);
        insert.setString(3, storPoolKind);
        insert.setString(4, nodeName);
        insert.setString(5, storPoolName);

        insert.executeUpdate();
    }

    private int createLayerRscIdEntry(
        PreparedStatement insert,
        String nodeName,
        String rscName,
        String snapName,
        String kind,
        @Nullable Integer parentId
    )
        throws SQLException
    {
        Key key = new Key(nodeName, rscName, snapName, kind);
        Integer idTmp = layerRscIdMap.get(key);
        int id;
        if (idTmp == null)
        {
            id = nextLRI++;
            insert.setInt(1, id);
            insert.setString(2, nodeName);
            insert.setString(3, rscName);
            insert.setString(4, snapName);
            insert.setString(5, kind);
            if (parentId == null)
            {
                insert.setNull(6, Types.INTEGER);
            }
            else
            {
                insert.setInt(6, parentId);
            }
            insert.setString(7, DFLT_RSC_NAME_SUFFIX);
            insert.executeUpdate();
            layerRscIdMap.put(key, id);
        }
        else
        {
            id = idTmp;
        }
        return id;
    }

    private void dropTable(Connection dbCon, String table) throws SQLException
    {
        try (PreparedStatement stmt = dbCon.prepareStatement("DROP TABLE " + table))
        {
            stmt.executeUpdate();
        }
    }

    private static class Key
    {
        private final Object[] keys;
        private final int hash;

        private Key(Object... keysRef)
        {
            keys = keysRef;
            final int prime = 31;
            int result = 1;
            hash = prime * result + Arrays.deepHashCode(keys);
        }

        @Override
        public int hashCode()
        {
            return hash;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = obj != null && obj instanceof Key;
            if (eq)
            {
                eq = Arrays.deepEquals(keys, ((Key) obj).keys);
            }
            return eq;
        }
    }
}
