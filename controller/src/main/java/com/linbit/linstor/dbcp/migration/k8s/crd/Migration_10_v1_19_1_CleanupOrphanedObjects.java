package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.SnapDfnKey;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.KeyValueStore;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.KeyValueStoreSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.NodeStorPool;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.NodeStorPoolSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainersSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourceDefinitions;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.ResourceDefinitionsSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.SecAclMap;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.SecAclMapSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.SecObjectProtection;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.SecObjectProtectionSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.StorPoolDefinitions;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.StorPoolDefinitionsSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.SPD_DFLT_DISKLESS_STOR_POOL;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.SPD_DFLT_STOR_POOL;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.getKvsToDelete;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.getSecObjPathsToDelete;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

@K8sCrdMigration(
    description = "Cleanup orphaned Snapshot and SnapshotVolume Properties",
    version = 10
)
public class Migration_10_v1_19_1_CleanupOrphanedObjects extends BaseK8sCrdMigration
{
    public Migration_10_v1_19_1_CleanupOrphanedObjects()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        cleanupStorPoolDefinitions(txFrom, txTo);
        cleanupSnapDfnSecObjects(txFrom, txTo);
        cleanupEmptyKvs(txFrom, txTo);

        return null;
    }

    private void cleanupStorPoolDefinitions(K8sCrdTransaction txFromRef, K8sCrdTransaction txToRef)
    {
        Collection<StorPoolDefinitionsSpec> storPoolDfns = txFromRef
            .<StorPoolDefinitions, StorPoolDefinitionsSpec>getSpec(
            GenCrdV1_19_1.GeneratedDatabaseTables.STOR_POOL_DEFINITIONS
        ).values();
        Collection<NodeStorPoolSpec> storPools = txFromRef.<NodeStorPool, NodeStorPoolSpec>getSpec(
            GenCrdV1_19_1.GeneratedDatabaseTables.NODE_STOR_POOL
        ).values();

        Map<String, StorPoolDefinitionsSpec> storPoolNameToSpec = new HashMap<>();
        for (StorPoolDefinitionsSpec spdSpec : storPoolDfns)
        {
            storPoolNameToSpec.put(spdSpec.poolName, spdSpec);
        }

        storPoolNameToSpec.remove(SPD_DFLT_STOR_POOL);
        storPoolNameToSpec.remove(SPD_DFLT_DISKLESS_STOR_POOL);
        for (NodeStorPoolSpec spSpec : storPools)
        {
            storPoolNameToSpec.remove(spSpec.poolName);
        }

        for (StorPoolDefinitionsSpec spdSpec : storPoolNameToSpec.values())
        {
            txToRef.delete(
                GenCrdV1_19_1.GeneratedDatabaseTables.STOR_POOL_DEFINITIONS,
                GenCrdV1_19_1.specToCrd(spdSpec)
            );
        }
    }

    private void cleanupSnapDfnSecObjects(K8sCrdTransaction txFromRef, K8sCrdTransaction txToRef)
    {
        Collection<ResourceDefinitionsSpec> snapDfns = txFromRef.<ResourceDefinitions, ResourceDefinitionsSpec>getSpec(
            GenCrdV1_19_1.GeneratedDatabaseTables.RESOURCE_DEFINITIONS,
            rd -> rd.getSpec().snapshotName != null && !rd.getSpec().snapshotName.isEmpty()
        ).values();
        Collection<SecObjectProtection> snapDfnSecObjProtPaths = txFromRef
            .<SecObjectProtection, SecObjectProtectionSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.SEC_OBJECT_PROTECTION,
            objProt -> objProt.getSpec().objectPath.startsWith("/snapshotdefinitions/")
        ).values();
        Collection<SecAclMap> snapDfnSecAclObjPaths = txFromRef.<SecAclMap, SecAclMapSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.SEC_ACL_MAP,
            secAclMap -> secAclMap.getSpec().objectPath.startsWith("/snapshotdefinitions/")
        ).values();

        HashSet<SnapDfnKey> knownSnapDfns = new HashSet<>();
        for (ResourceDefinitionsSpec snapDfnSpec : snapDfns)
        {
            knownSnapDfns.add(new SnapDfnKey(snapDfnSpec.resourceName, snapDfnSpec.snapshotName));
        }

        HashMap<String, SecObjectProtection> pathToSecObjProt = mapBy(
            snapDfnSecObjProtPaths,
            objProt -> objProt.getSpec().objectPath
        );

        HashMap<String, SecAclMap> pathToSecAclMap = mapBy(
            snapDfnSecAclObjPaths,
            objProt -> objProt.getSpec().objectPath
        );

        for (String secObjPathToDelete : getSecObjPathsToDelete(knownSnapDfns, pathToSecObjProt.keySet()))
        {
            txToRef.delete(
                GenCrdV1_19_1.GeneratedDatabaseTables.SEC_OBJECT_PROTECTION,
                pathToSecObjProt.get(secObjPathToDelete)
            );
        }
        for (String secObjPathToDelete : getSecObjPathsToDelete(knownSnapDfns, pathToSecAclMap.keySet()))
        {
            txToRef.delete(GenCrdV1_19_1.GeneratedDatabaseTables.SEC_ACL_MAP, pathToSecAclMap.get(secObjPathToDelete));
        }
    }

    private <T> HashMap<String, T> mapBy(
        Collection<T> collectionRef,
        Function<T, String> mappingFktRef
    )
    {
        HashMap<String, T> ret = new HashMap<>();
        for (T t : collectionRef)
        {
            ret.put(mappingFktRef.apply(t), t);
        }
        return ret;
    }

    private void cleanupEmptyKvs(K8sCrdTransaction txFromRef, K8sCrdTransaction txToRef)
    {
        Collection<KeyValueStore> keyValueStores = txFromRef.<KeyValueStore, KeyValueStoreSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.KEY_VALUE_STORE
        ).values();
        Collection<PropsContainers> propsContainers = txFromRef.<PropsContainers, PropsContainersSpec>getCrd(
            GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
            propsCrd -> propsCrd.getSpec().propsInstance.startsWith("/keyvaluestores/")
        ).values();

        HashMap<String, KeyValueStore> kvsNameToInstance = mapBy(keyValueStores, kvs -> kvs.getSpec().kvsName);
        HashMap<String, PropsContainers> propsInstanceNameToPropsContainer = mapBy(
            propsContainers,
            prop -> prop.getSpec().propsInstance
        );

        HashSet<String> kvsToDelete = getKvsToDelete(
            kvsNameToInstance.keySet(),
            propsInstanceNameToPropsContainer.keySet()
        );
        for (String kvsNameToDelete : kvsToDelete)
        {
            txToRef.delete(
                GenCrdV1_19_1.GeneratedDatabaseTables.KEY_VALUE_STORE,
                kvsNameToInstance.get(kvsNameToDelete)
            );
        }
    }
}
