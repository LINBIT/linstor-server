package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.SnapDfnKey;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import static com.linbit.linstor.dbcp.migration.Migration_2022_10_03_CleanupOrphanedSnapAndSnapVlmProps.PATH_SEPARATOR;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.SPD_DFLT_DISKLESS_STOR_POOL;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.SPD_DFLT_STOR_POOL;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.TBL_KVS;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.TBL_PROPS_CON;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.TBL_RD;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.TBL_SEC_ACL_MAP;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.TBL_SEC_OBJ_PROT;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.TBL_STOR_POOLS;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.TBL_STOR_POOL_DEFINITIONS;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.getKvsToDelete;
import static com.linbit.linstor.dbcp.migration.Migration_2022_11_14_CleanupOrphanedObjects.getSecObjPathsToDelete;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

@EtcdMigration(
    description = "Cleanup orphaned Objects",
    version = 50
)
public class Migration_23_CleanupOrphanedObjects extends BaseEtcdMigration
{

    @Override
    public void migrate(EtcdTransaction tx, String prefix) throws Exception
    {
        cleanupStorPoolDfnsWithNoStorPools(tx, prefix);
        cleanupSnapshotDfnSecObjects(tx, prefix);
        cleanupEmptyKvs(tx, prefix);
    }

    private void cleanupStorPoolDfnsWithNoStorPools(EtcdTransaction tx, String prefix)
    {
        String storPoolDfnTbl = prefix + TBL_STOR_POOL_DEFINITIONS + PATH_SEPARATOR;
        Set<String> storPoolDfnsToDelete = getSinglePkEntries(
            tx,
            storPoolDfnTbl,
            0
        );
        Set<String> allStorPoolNames = getSinglePkEntries(
            tx,
            prefix + TBL_STOR_POOLS + PATH_SEPARATOR,
            1
        );
        storPoolDfnsToDelete.removeAll(allStorPoolNames);
        storPoolDfnsToDelete.remove(SPD_DFLT_STOR_POOL);
        storPoolDfnsToDelete.remove(SPD_DFLT_DISKLESS_STOR_POOL);
        for (String storPoolDfnToDelete : storPoolDfnsToDelete)
        {
            tx.delete(storPoolDfnTbl + storPoolDfnToDelete, true);
        }
    }

    private void cleanupSnapshotDfnSecObjects(EtcdTransaction txRef, String prefixRef)
    {
        final String secObjPathTbl = prefixRef + TBL_SEC_OBJ_PROT + PATH_SEPARATOR;
        final String secAclTbl = prefixRef + TBL_SEC_ACL_MAP + PATH_SEPARATOR;

        HashSet<SnapDfnKey> knownSnapDfns = getPks(
            txRef,
            prefixRef + TBL_RD + PATH_SEPARATOR,
            arr -> new SnapDfnKey(arr[0], arr[1]),
            key -> key.snapName != null || !key.snapName.isEmpty()
        );
        HashSet<String> snapDfnSecObjPaths = getSinglePkEntries(
            txRef,
            secObjPathTbl,
            0,
            objPath -> objPath.startsWith("/snapshotdefinitions/")
        );
        HashSet<String> snapDfnSecAclObjPaths = getSinglePkEntries(
            txRef,
            secAclTbl,
            0,
            objPath -> objPath.startsWith("/snapshotdefinitions/")
        );

        for (String secObjPathToDelete : getSecObjPathsToDelete(knownSnapDfns, snapDfnSecObjPaths))
        {
            txRef.delete(secObjPathTbl + secObjPathToDelete, true);
        }
        for (String secObjPathToDelete : getSecObjPathsToDelete(knownSnapDfns, snapDfnSecAclObjPaths))
        {
            txRef.delete(secAclTbl + secObjPathToDelete, true);
        }
    }

    private void cleanupEmptyKvs(EtcdTransaction txRef, String prefixRef)
    {
        final String kvsTbl = prefixRef + TBL_KVS + PATH_SEPARATOR;
        HashSet<String> kvsNameSet = getSinglePkEntries(
            txRef,
            kvsTbl,
            0
        );
        HashSet<String> propsInstanceSet = getSinglePkEntries(
            txRef,
            prefixRef + TBL_PROPS_CON + PATH_SEPARATOR,
            0,
            objPath -> objPath.startsWith("/keyvaluestores/")
        );

        HashSet<String> kvsToDelete = getKvsToDelete(kvsNameSet, propsInstanceSet);

        for (String kvsName : kvsToDelete)
        {
            txRef.delete(kvsTbl + kvsName, true);
        }
    }

    private HashSet<String> getSinglePkEntries(final EtcdTransaction tx, final String tablePrefix, final int pkIdx)
    {
        return getPks(tx, tablePrefix, arr -> arr[pkIdx], ignored -> true);
    }

    private HashSet<String> getSinglePkEntries(
        final EtcdTransaction tx,
        final String tablePrefix,
        final int pkIdx,
        Predicate<String> filter
    )
    {
        return getPks(tx, tablePrefix, arr -> arr[pkIdx], filter);
    }

    private <T> HashSet<T> getPks(
        final EtcdTransaction tx,
        final String tablePrefix,
        Function<String[], T> mappingFkt,
        Predicate<T> filter
    )
    {
        HashSet<T> ret = new HashSet<>();
        TreeMap<String, String> treeMap = tx.get(tablePrefix);
        for (String fullEtcdKey : treeMap.keySet())
        {
            // key: "${prefix}/${tableName}/${pk0}:${pk1}:.../${columnName}"
            T mappedObj = mappingFkt.apply(EtcdUtils.splitPks(extractPrimaryKey(fullEtcdKey), false));
            if (filter.test(mappedObj))
            {
                ret.add(mappedObj);
            }
        }
        return ret;
    }

}
