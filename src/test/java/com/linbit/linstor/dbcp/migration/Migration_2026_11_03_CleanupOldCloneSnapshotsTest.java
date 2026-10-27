package com.linbit.linstor.dbcp.migration;

import java.util.UUID;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:typename")
public class Migration_2026_11_03_CleanupOldCloneSnapshotsTest extends MigrationTestBase
{
    private static final String PRE_VERSION = "2026.07.03.10.00";
    private static final String TEST_VERSION = "2026.11.03.10.00";

    private static final String STLT_MIG_PROP = Migration_2026_11_03_CleanupOldCloneSnapshots.PROP_KEY_STLT_MIGRATION;

    @Test
    public void satelliteMigrationOnlyForNodesWithLvmStorPools() throws Exception
    {
        startDbAtVersion("cleanupOldCloneSnapsStltMig", PRE_VERSION);
        createStorPool("LVMNODE", "LVMPOOL", "LVM");
        createStorPool("THINNODE", "THINPOOL", "LVM_THIN");
        createStorPool("ZFSNODE", "ZFSPOOL", "ZFS");
        // both kinds of LVM storage pools on one node
        createStorPool("BOTHNODE", "LVMPOOL", "LVM");
        createStorPool("BOTHNODE", "THINPOOL", "LVM_THIN");

        migrateTo(TEST_VERSION);

        for (String nodeName : new String[] {"LVMNODE", "THINNODE", "BOTHNODE"})
        {
            assertThat(querySingleString(selectProp("/NODES/" + nodeName, STLT_MIG_PROP))).isEqualTo("True");
        }
        assertThat(countRows("PROPS_CONTAINERS WHERE PROPS_INSTANCE = '/NODES/ZFSNODE'")).isZero();
    }

    @Test
    public void deletesOldClonePropsOfResources() throws Exception
    {
        startDbAtVersion("cleanupOldCloneSnapsProps", PRE_VERSION);
        executeUpdate(
            "INSERT INTO PROPS_CONTAINERS (PROPS_INSTANCE, PROP_KEY, PROP_VALUE) VALUES" +
                " ('/RSCS/NODE/SRC', 'Clone/CF_target1', 'target1')," +
                " ('/RSCS/NODE/SRC', 'Clone/CF_target2', 'target2')," +
                // a user property of the clone namespace
                " ('/RSCS/NODE/SRC', 'Clone/DdBlocksize', '1M')," +
                // "_" is a wildcard of LIKE, this one does not have the prefix
                " ('/RSCS/NODE/SRC', 'Clone/CFXtarget3', 'target3')"
        );

        migrateTo(TEST_VERSION);

        assertThat(countRows("PROPS_CONTAINERS WHERE PROP_KEY LIKE 'Clone/CF%'")).isEqualTo(1);
        assertThat(querySingleString(selectProp("/RSCS/NODE/SRC", "Clone/CFXtarget3"))).isEqualTo("target3");
        assertThat(querySingleString(selectProp("/RSCS/NODE/SRC", "Clone/DdBlocksize"))).isEqualTo("1M");
    }

    private void createStorPool(String nodeName, String poolName, String driverName) throws Exception
    {
        if (countRows("NODES WHERE NODE_NAME = '" + nodeName + "'") == 0)
        {
            executeUpdate(
                "INSERT INTO NODES (UUID, NODE_NAME, NODE_DSP_NAME, NODE_FLAGS, NODE_TYPE) VALUES" +
                    " ('" + UUID.randomUUID() + "', '" + nodeName + "', '" + nodeName + "', 0, 2)"
            );
        }
        if (countRows("STOR_POOL_DEFINITIONS WHERE POOL_NAME = '" + poolName + "'") == 0)
        {
            executeUpdate(
                "INSERT INTO STOR_POOL_DEFINITIONS (UUID, POOL_NAME, POOL_DSP_NAME) VALUES" +
                    " ('" + UUID.randomUUID() + "', '" + poolName + "', '" + poolName + "')"
            );
        }
        executeUpdate(
            "INSERT INTO NODE_STOR_POOL (UUID, NODE_NAME, POOL_NAME, DRIVER_NAME) VALUES" +
                " ('" + UUID.randomUUID() + "', '" + nodeName + "', '" + poolName + "', '" + driverName + "')"
        );
    }

    private String selectProp(String instance, String key)
    {
        return "SELECT PROP_VALUE FROM PROPS_CONTAINERS WHERE PROPS_INSTANCE = '" + instance +
            "' AND PROP_KEY = '" + key + "'";
    }
}
