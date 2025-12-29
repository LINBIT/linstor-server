package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

@Migration(
    version = "2023.07.26.09.00",
    description = "Cleanup orphaned ACL entries"
)
public class Migration_2023_07_26_CleanupOrphanedAcls extends LinstorMigration
{
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

    private static final String TBL_SEC_ACL = "SEC_ACL_MAP";

    private static final String TBL_RSC = "RESOURCES";
    private static final String TBL_RSC_DFN = "RESOURCE_DEFINITIONS";
    private static final String TBL_RSC_GRP = "RESOURCE_GROUPS";
    private static final String TBL_SCHEDULES = "SCHEDULES";
    private static final String TBL_NODES = "NODES";
    private static final String TBL_STOR_POOL_DFN = "STOR_POOL_DEFINITIONS";
    private static final String TBL_KVS = "KEY_VALUE_STORE";
    private static final String TBL_EXT_FILES = "FILES";
    private static final String TBL_EBS_REMOTES = "EBS_REMOTES";
    private static final String TBL_LINSTOR_REMOTES = "LINSTOR_REMOTES";
    private static final String TBL_S3_REMOTES = "S3_REMOTES";

    private static final String CLM_NODE_NAME = "NODE_NAME";
    private static final String CLM_RSC_NAME = "RESOURCE_NAME";
    private static final String CLM_SNAP_NAME = "SNAPSHOT_NAME";
    private static final String CLM_RSC_GRP_NAME = "RESOURCE_GROUP_NAME";
    private static final String CLM_POOL_NAME = "POOL_NAME";
    private static final String CLM_KVS_NAME = "KVS_NAME";
    private static final String CLM_PATH = "PATH";
    private static final String CLM_NAME = "NAME";

    private static final String CLM_OBJECT_PATH = "OBJECT_PATH";
    private static final String CLM_ROLE_NAME = "ROLE_NAME";

    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        TreeSet<String> neededObjPaths = new TreeSet<>();

        neededObjPaths.addAll(
            fromBiPkTbl(conRef, TBL_RSC, CLM_NODE_NAME, CLM_RSC_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathRsc)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_RSC_DFN, CLM_RSC_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathRscDfn)
        );
        neededObjPaths.addAll(
            fromBiPkTbl(conRef, TBL_RSC_DFN, CLM_RSC_NAME, CLM_SNAP_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathRsc)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_RSC_GRP, CLM_RSC_GRP_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathRscGrp)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_NODES, CLM_NODE_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathNode)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(
                conRef,
                TBL_STOR_POOL_DFN,
                CLM_POOL_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathStorPoolDfn
            )
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_KVS, CLM_KVS_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathKvs)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_EXT_FILES, CLM_PATH,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathExtFile)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_EBS_REMOTES, CLM_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathRemote)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_LINSTOR_REMOTES, CLM_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathRemote)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_S3_REMOTES, CLM_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathRemote)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_SCHEDULES, CLM_NAME,
                Migration_2023_07_26_CleanupOrphanedAcls::objPathSchedule)
        );


        try (
            PreparedStatement selectStmt = conRef.prepareStatement(
                "SELECT " + CLM_OBJECT_PATH + ", " + CLM_ROLE_NAME + " FROM " + TBL_SEC_ACL,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE
            ))
        {
            ResultSet resultSet = selectStmt.executeQuery();
            while (resultSet.next())
            {
                if (!Migration_2023_07_26_CleanupOrphanedAcls.isObjPathNeeded(
                    resultSet.getString(CLM_OBJECT_PATH),
                    neededObjPaths
                ))
                {
                    resultSet.deleteRow();
                }
            }
        }
    }

    private Collection<String> fromSinglePkTbl(
        Connection conRef,
        String tblNameRef,
        String pkClmNameRef,
        UnaryOperator<String> pkToObjPathFuncRef
    )
        throws SQLException
    {
        HashSet<String> ret = new HashSet<>();
        try (PreparedStatement prepStmt = conRef.prepareStatement("SELECT " + pkClmNameRef + " FROM " + tblNameRef))
        {
            ResultSet resultSet = prepStmt.executeQuery();
            while (resultSet.next())
            {
                ret.add(pkToObjPathFuncRef.apply(resultSet.getString(pkClmNameRef)));
            }
        }
        return ret;
    }

    private Collection<String> fromBiPkTbl(
        Connection conRef,
        String tblNameRef,
        String pk1ClmNameRef,
        String pk2ClmNameRef,
        BinaryOperator<String> pkToObjPathFuncRef
    )
        throws SQLException
    {
        HashSet<String> ret = new HashSet<>();
        try (
            PreparedStatement prepStmt = conRef.prepareStatement(
                "SELECT " + pk1ClmNameRef + ", " + pk2ClmNameRef + " FROM " + tblNameRef
            ))
        {
            ResultSet resultSet = prepStmt.executeQuery();
            while (resultSet.next())
            {
                ret.add(
                    pkToObjPathFuncRef.apply(resultSet.getString(pk1ClmNameRef), resultSet.getString(pk2ClmNameRef))
                );
            }
        }
        return ret;
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
