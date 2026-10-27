package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbcp.migration.Migration_2026_11_03_CleanupOldCloneSnapshots;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.NodeStorPool;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainersSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Migration_37_v1_34_0_CleanupOldCloneSnapshotsTest
{
    private static final DatabaseTable NODE_STOR_POOL = GenCrdV1_34_0.GeneratedDatabaseTables.NODE_STOR_POOL;
    private static final DatabaseTable PROPS_CONTAINERS = GenCrdV1_34_0.GeneratedDatabaseTables.PROPS_CONTAINERS;
    private static final String STLT_MIG_PROP = Migration_2026_11_03_CleanupOldCloneSnapshots.PROP_KEY_STLT_MIGRATION;

    private FakeK8sResourceClient<NodeStorPool> storPoolClient;
    private FakeK8sResourceClient<PropsContainers> propsClient;
    private K8sCrdTransaction tx;

    @Before
    public void setUp()
    {
        storPoolClient = new FakeK8sResourceClient<>();
        propsClient = new FakeK8sResourceClient<>();
        Map<DatabaseTable, Supplier<K8sResourceClient<?>>> clients = new HashMap<>();
        clients.put(NODE_STOR_POOL, () -> storPoolClient);
        clients.put(PROPS_CONTAINERS, () -> propsClient);
        tx = new K8sCrdTransaction(clients, null, null, GenCrdV1_34_0.VERSION);
    }

    @Test
    public void satelliteMigrationOnlyForNodesWithLvmStorPools()
    {
        addStorPool("LVMNODE", "LVMPOOL", "LVM");
        addStorPool("THINNODE", "THINPOOL", "LVM_THIN");
        addStorPool("ZFSNODE", "ZFSPOOL", "ZFS");
        // both kinds of LVM storage pools on one node
        addStorPool("BOTHNODE", "LVMPOOL", "LVM");
        addStorPool("BOTHNODE", "THINPOOL", "LVM_THIN");

        Migration_37_v1_34_0_CleanupOldCloneSnapshots.cleanup(tx);

        assertThat(props()).containsOnlyKeys(
            stltMigProp("LVMNODE").getK8sKey(),
            stltMigProp("THINNODE").getK8sKey(),
            stltMigProp("BOTHNODE").getK8sKey()
        );
    }

    @Test
    public void deletesOldClonePropsOfResources()
    {
        PropsContainers userProp = GenCrdV1_34_0.createPropsContainers("/RSCS/NODE/SRC", "Clone/DdBlocksize", "1M");
        PropsContainers otherProp = GenCrdV1_34_0.createPropsContainers("/RSCS/NODE/SRC", "Clone/CFXtarget", "target");
        propsClient.add(GenCrdV1_34_0.createPropsContainers("/RSCS/NODE/SRC", "Clone/CF_target1", "target1"));
        propsClient.add(GenCrdV1_34_0.createPropsContainers("/RSCS/NODE/SRC", "Clone/CF_target2", "target2"));
        propsClient.add(userProp);
        propsClient.add(otherProp);

        Migration_37_v1_34_0_CleanupOldCloneSnapshots.cleanup(tx);

        assertThat(props()).containsOnlyKeys(userProp.getK8sKey(), otherProp.getK8sKey());
    }

    private void addStorPool(String nodeName, String poolName, String driverName)
    {
        storPoolClient.add(
            GenCrdV1_34_0.createNodeStorPool(
                UUID.randomUUID().toString(),
                nodeName,
                poolName,
                driverName,
                "SYSTEM:",
                "SYSTEM:",
                false
            )
        );
    }

    private static PropsContainers stltMigProp(String nodeName)
    {
        return GenCrdV1_34_0.createPropsContainers(
            "/NODES/" + nodeName,
            STLT_MIG_PROP,
            Migration_2026_11_03_CleanupOldCloneSnapshots.PROP_VALUE_STLT_MIGRATION
        );
    }

    private HashMap<String, PropsContainers> props()
    {
        return tx.<PropsContainers, PropsContainersSpec>getCrd(PROPS_CONTAINERS);
    }
}
