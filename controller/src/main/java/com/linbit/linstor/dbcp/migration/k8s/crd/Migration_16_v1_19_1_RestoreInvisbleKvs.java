package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.KeyValueStore;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.KeyValueStoreSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainersSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;

@K8sCrdMigration(
    description = "Restore invisible KVS",
    version = 16
)
public class Migration_16_v1_19_1_RestoreInvisbleKvs extends BaseK8sCrdMigration
{
    /*
     * Migration_10_v1_19_1_CleanupOrphanedObjects accidentally deleted all KVS entries from the KVS table, but not
     * the props.
     * The problem was that the migration scanned for KVS with no properties, but was looking for lower-cased
     * PROPS_INSTANCE entries although the driver uppercases the PROPS_INSTANCE always.
     *
     * The props were not touched, but since the entries in the KVS table was missing,
     * "linstor key-value-store list" did not list them and "linstor key-value-store show <kvs>" also did not
     * contain any entries since they were simply not loaded from the database
     */

    private static final String PROP_INSTANCE_PREFIX = "/KEYVALUESTORES/";

    public Migration_16_v1_19_1_RestoreInvisbleKvs()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        HashSet<String> kvsFromProps = new HashSet<>();

        Collection<PropsContainers> propsCrdList = txFrom.<PropsContainers, PropsContainersSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS
        ).values();

        for (PropsContainers props : propsCrdList)
        {
            PropsContainersSpec spec = props.getSpec();
            if (spec.propsInstance.startsWith(PROP_INSTANCE_PREFIX))
            {
                kvsFromProps.add(spec.propsInstance.substring(PROP_INSTANCE_PREFIX.length()));
            }
        }

        Collection<KeyValueStore> kvsCrdList = txFrom.<KeyValueStore, KeyValueStoreSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.KEY_VALUE_STORE
        ).values();
        for (KeyValueStore kvs : kvsCrdList)
        {
            kvsFromProps.remove(kvs.getSpec().kvsName);
        }

        for (String kvsToRestore : kvsFromProps)
        {
            txTo.create(
                GenCrdV1_19_1.GeneratedDatabaseTables.KEY_VALUE_STORE,
                GenCrdV1_19_1.createKeyValueStore(
                    UUID.randomUUID().toString(),
                    kvsToRestore,
                    kvsToRestore.toLowerCase()
                )
            );
        }

        return null;
    }
}
