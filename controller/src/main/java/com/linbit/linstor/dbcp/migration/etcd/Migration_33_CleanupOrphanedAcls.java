package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

@EtcdMigration(
    description = "Cleanup orphaned ACL entries",
    version = 60
)
public class Migration_33_CleanupOrphanedAcls extends BaseEtcdMigration
{
    /*
     * < v1.18.0 ACL entries were not properly deleted.
     * >= v1.18.0 the drivers simply ignored the unnecessary entries, since the entries were only loaded on demand when
     * the objProt was also loaded. Since the objProt were properly deleted, nobody was bothered with the unnecessary
     * ACL entries. Since the loading procedure changed with the new drivers, those orphaned entries causes problems.
     */

    private static final String TBL_SEC_ACL = "SEC_ACL_MAP/";

    private static final String TBL_RSC = "RESOURCES/";
    private static final String TBL_RSC_DFN = "RESOURCE_DEFINITIONS/";
    private static final String TBL_RSC_GRP = "RESOURCE_GROUPS/";
    private static final String TBL_SCHEDULES = "SCHEDULES/";
    private static final String TBL_NODES = "NODES/";
    private static final String TBL_STOR_POOL_DFN = "STOR_POOL_DEFINITIONS/";
    private static final String TBL_KVS = "KEY_VALUE_STORE/";
    private static final String TBL_EXT_FILES = "FILES/";
    private static final String TBL_EBS_REMOTES = "EBS_REMOTES/";
    private static final String TBL_LINSTOR_REMOTES = "LINSTOR_REMOTES/";
    private static final String TBL_S3_REMOTES = "S3_REMOTES/";

    private static final String PATH_SYS_PREFIX = "/sys/";

    private static final String PATH_RESOURCES = "/resources/";
    private static final String PATH_RESOURCE_DEFINITIONS = "/resourcedefinitions/";
    private static final String PATH_SNAPSHOT_DEFINITIONS = "/snapshotdefinitions/";
    private static final String PATH_RESOURCE_GROUPS = "/resourcegroups/";
    private static final String PATH_NODES = "/nodes/";
    private static final String PATH_STOR_POOL_DEFINITIONS = "/storpooldefinitions/";
    private static final String PATH_FREE_SPACE_MGRS = "/freespacemgrs/";
    private static final String PATH_KVS = "/keyvaluestores/";
    private static final String PATH_EXT_FILES = "/extfiles/";
    private static final String PATH_REMOTE = "/remote/";
    private static final String PATH_SCHEDULE = "/schedule/";

    private static final String PATH_SEPARATOR = "/";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        String prefixedTblKey = prefix + TBL_SEC_ACL;
        int prefixedTblKeyLen = prefixedTblKey.length();

        Map<String, Function<String, String>> tableAndHandlers = new TreeMap<>();
        tableAndHandlers.put(TBL_RSC, Migration_33_CleanupOrphanedAcls::composedPkToRscObjPath);
        tableAndHandlers.put(TBL_RSC_DFN, Migration_33_CleanupOrphanedAcls::objPathRscDfn);
        tableAndHandlers.put(TBL_RSC_GRP, Migration_33_CleanupOrphanedAcls::objPathRscGrp);
        tableAndHandlers.put(TBL_NODES, Migration_33_CleanupOrphanedAcls::objPathNode);
        tableAndHandlers.put(TBL_STOR_POOL_DFN, Migration_33_CleanupOrphanedAcls::objPathStorPoolDfn);
        tableAndHandlers.put(TBL_KVS, Migration_33_CleanupOrphanedAcls::objPathKvs);
        tableAndHandlers.put(TBL_EXT_FILES, Migration_33_CleanupOrphanedAcls::objPathExtFile);
        tableAndHandlers.put(TBL_EBS_REMOTES, Migration_33_CleanupOrphanedAcls::objPathRemote);
        tableAndHandlers.put(TBL_LINSTOR_REMOTES, Migration_33_CleanupOrphanedAcls::objPathRemote);
        tableAndHandlers.put(TBL_S3_REMOTES, Migration_33_CleanupOrphanedAcls::objPathRemote);
        tableAndHandlers.put(TBL_SCHEDULES, Migration_33_CleanupOrphanedAcls::objPathSchedule);


        TreeSet<String> neededObjPaths = new TreeSet<>();
        for (Entry<String, Function<String, String>> entry : tableAndHandlers.entrySet())
        {
            neededObjPaths.addAll(getGenericObjProt(tx, prefix, entry.getKey(), entry.getValue()));
        }
        // also add SnapDfn's objPath. could not use the tableAndHandlers from before since snapDfn and rscDfn have the
        // same key in that map
        neededObjPaths.addAll(
            getGenericObjProt(
                tx,
                prefix,
                TBL_RSC_DFN,
                Migration_33_CleanupOrphanedAcls::composedPkToSnapDfnPath
            )
        );

        TreeMap<String, String> data = tx.get(prefixedTblKey);
        for (String oldFullEtcdKey : data.keySet())
        {
            String combinedPkAndColumn = oldFullEtcdKey.substring(prefixedTblKeyLen);
            String combinedPk = combinedPkAndColumn.substring(0, combinedPkAndColumn.lastIndexOf("/"));

            String[] pks = combinedPk.split(":");
            // we dont really care about [1] since that is the ROLE_NAME.
            // we need [0] (the OBJECT_PATH) to figure out if the corresponding object (node, rsc, ...) still exists
            String objPath = pks[0];

            if (!isObjPathNeeded(objPath, neededObjPaths))
            {
                tx.delete(oldFullEtcdKey);
            }
        }
    }

    public static boolean isObjPathNeeded(String objPathRef, TreeSet<String> neededObjPathsRef)
    {
        boolean ret = objPathRef.startsWith(PATH_SYS_PREFIX);
        if (!ret)
        {
            ret = neededObjPathsRef.contains(objPathRef);
        }
        return ret;
    }

    private static String composedPkToRscObjPath(String composedPkRef)
    {
        String[] pks = composedPkRef.split(":");
        // if [2] is also set (snap_name), it does not matter if we still add it to our return set since we cannot
        // delete what does not exist. this migration will certainly not start creating "missing" objPaths / ACL entries
        return objPathRsc(pks[0], pks[1]);
    }

    private static String composedPkToSnapDfnPath(String composedPkRef)
    {
        String[] pks = composedPkRef.split(":");

        String ret;
        if (pks.length == 2)
        {
            ret = objPathSnapDfn(pks[0], pks[1]);
        }
        else
        {
            ret = ""; // so we can ignore the nullcheck when adding to the "needed objPaths" set
        }
        return ret;
    }

    private Collection<String> getGenericObjProt(
        EtcdTransaction txRef,
        String prefixRef,
        String tblNameRef,
        Function<String, String> composedPkToObjPathFuncRef
    )
    {
        String prefixedTblName = prefixRef + tblNameRef;
        int prefixedTblNameLen = prefixedTblName.length();

        HashSet<String> ret = new HashSet<>();
        TreeMap<String, String> data = txRef.get(prefixedTblName, true);
        for (String fullEtcdKey : data.keySet())
        {
            String composedPk = fullEtcdKey.substring(prefixedTblNameLen, fullEtcdKey.lastIndexOf("/"));
            ret.add(composedPkToObjPathFuncRef.apply(composedPk));
        }
        return ret;
    }


    public static String objPathRsc(String nodeNameRef, String rscNameRef)
    {
        return PATH_RESOURCES + nodeNameRef + PATH_SEPARATOR + rscNameRef;
    }

    public static String objPathRscDfn(String rscName)
    {
        return PATH_RESOURCE_DEFINITIONS + rscName;
    }

    public static String objPathSnapDfn(String rscNameRef, String snapNameRef)
    {
        return PATH_SNAPSHOT_DEFINITIONS + rscNameRef + PATH_SEPARATOR + snapNameRef;
    }

    public static String objPathRscGrp(String rscGrpNameRef)
    {
        return PATH_RESOURCE_GROUPS + rscGrpNameRef;
    }

    public static String objPathNode(String nodeNameRef)
    {
        return PATH_NODES + nodeNameRef;
    }

    public static String objPathStorPoolDfn(String storPoolNameRef)
    {
        return PATH_STOR_POOL_DEFINITIONS + storPoolNameRef;
    }

    public static String objPathKvs(String kvsNameRef)
    {
        return PATH_KVS + kvsNameRef;
    }

    public static String objPathFsm(String fsmNameRef)
    {
        return PATH_FREE_SPACE_MGRS + fsmNameRef;
    }

    public static String objPathExtFile(String extFileNameRef)
    {
        return PATH_EXT_FILES + extFileNameRef;
    }

    public static String objPathRemote(String remoteNameRef)
    {
        return PATH_REMOTE + remoteNameRef;
    }

    public static String objPathSchedule(String scheduleNameRef)
    {
        return PATH_SCHEDULE + scheduleNameRef;
    }
}
