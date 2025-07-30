package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbcp.migration.etcd.Migration_33_CleanupOrphanedAcls;

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
            fromBiPkTbl(conRef, TBL_RSC, CLM_NODE_NAME, CLM_RSC_NAME, Migration_33_CleanupOrphanedAcls::objPathRsc)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_RSC_DFN, CLM_RSC_NAME, Migration_33_CleanupOrphanedAcls::objPathRscDfn)
        );
        neededObjPaths.addAll(
            fromBiPkTbl(conRef, TBL_RSC_DFN, CLM_RSC_NAME, CLM_SNAP_NAME, Migration_33_CleanupOrphanedAcls::objPathRsc)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_RSC_GRP, CLM_RSC_GRP_NAME, Migration_33_CleanupOrphanedAcls::objPathRscGrp)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_NODES, CLM_NODE_NAME, Migration_33_CleanupOrphanedAcls::objPathNode)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(
                conRef,
                TBL_STOR_POOL_DFN,
                CLM_POOL_NAME,
                Migration_33_CleanupOrphanedAcls::objPathStorPoolDfn
            )
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_KVS, CLM_KVS_NAME, Migration_33_CleanupOrphanedAcls::objPathKvs)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_EXT_FILES, CLM_PATH, Migration_33_CleanupOrphanedAcls::objPathExtFile)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_EBS_REMOTES, CLM_NAME, Migration_33_CleanupOrphanedAcls::objPathRemote)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_LINSTOR_REMOTES, CLM_NAME, Migration_33_CleanupOrphanedAcls::objPathRemote)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_S3_REMOTES, CLM_NAME, Migration_33_CleanupOrphanedAcls::objPathRemote)
        );
        neededObjPaths.addAll(
            fromSinglePkTbl(conRef, TBL_SCHEDULES, CLM_NAME, Migration_33_CleanupOrphanedAcls::objPathSchedule)
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
                if (!Migration_33_CleanupOrphanedAcls.isObjPathNeeded(
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
}
