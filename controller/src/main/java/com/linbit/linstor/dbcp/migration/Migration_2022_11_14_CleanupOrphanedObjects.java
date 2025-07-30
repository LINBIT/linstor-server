package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Migration(
    version = "2022.11.14.09.00",
    description = "Cleanup orphaned Objects"
)
public class Migration_2022_11_14_CleanupOrphanedObjects extends LinstorMigration
{
    public static final String TBL_STOR_POOL_DEFINITIONS = "STOR_POOL_DEFINITIONS";
    public static final String SPD_NAME = "POOL_NAME";
    public static final String SPD_DFLT_STOR_POOL = "DFLTSTORPOOL";
    public static final String SPD_DFLT_DISKLESS_STOR_POOL = "DFLTDISKLESSSTORPOOL";

    public static final String TBL_STOR_POOLS = "NODE_STOR_POOL";
    public static final String SP_NAME = "POOL_NAME";

    public static final String TBL_RD = "RESOURCE_DEFINITIONS";
    public static final String RD_RSC_NAME = "RESOURCE_NAME";
    public static final String RD_SNAP_NAME = "SNAPSHOT_NAME";

    public static final String TBL_SEC_OBJ_PROT = "SEC_OBJECT_PROTECTION";
    public static final String SEC_OBJ_PROT_PATH = "OBJECT_PATH";

    public static final String TBL_SEC_ACL_MAP = "SEC_ACL_MAP";
    public static final String SEC_ACL_OBJ_PATH = "OBJECT_PATH";

    public static final String TBL_KVS = "KEY_VALUE_STORE";
    public static final String KVS_NAME = "KVS_NAME";

    public static final String TBL_PROPS_CON = "PROPS_CONTAINERS";
    public static final String PROPS_INSTANCE = "PROPS_INSTANCE";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        cleanupStorPoolDfnsWithNoStorPools(connection);
        cleanupSnapshotDfnSecObjects(connection);
        cleanupEmptyKvs(connection);
    }

    /**
     * Leftovers: ObjectProtection and StoragePoolDefinition
     */
    private void cleanupStorPoolDfnsWithNoStorPools(Connection con) throws SQLException
    {

        try (
            PreparedStatement deleteOrphanedSPDStmt = con.prepareStatement(
                "DELETE FROM " + TBL_STOR_POOL_DEFINITIONS +
                    " WHERE " +
                    "   " + SPD_NAME + " <> '" + SPD_DFLT_STOR_POOL + "' AND " +
                    "   " + SPD_NAME + " <> '" + SPD_DFLT_DISKLESS_STOR_POOL + "' AND " +
                    "   " + SPD_NAME + " NOT IN (SELECT " + SP_NAME + " FROM " + TBL_STOR_POOLS + ")"
            ))
        {
            deleteOrphanedSPDStmt.execute();
        }
    }

    /**
     * Leftovers: ObjProt and ACL entries from already deleted snapshot-definitions
     */
    private void cleanupSnapshotDfnSecObjects(Connection con) throws SQLException
    {
        try (
            PreparedStatement getAllSnapDfns = con.prepareStatement(
                "SELECT " + RD_RSC_NAME + ", " + RD_SNAP_NAME +
                " FROM " + TBL_RD +
                " WHERE " + RD_SNAP_NAME + " IS NOT NULL" +
                  " AND " + RD_SNAP_NAME + " <> ''"
            );
            PreparedStatement getAllSnapDfnSecObjProt = con.prepareStatement(
                "SELECT " + SEC_OBJ_PROT_PATH +
                " FROM " + TBL_SEC_OBJ_PROT +
                " WHERE " + SEC_OBJ_PROT_PATH + " LIKE '/snapshotdefinitions/%'"
            );
            PreparedStatement getAllSnapDfnAcl = con.prepareStatement(
                "SELECT " + SEC_ACL_OBJ_PATH +
                " FROM " + TBL_SEC_ACL_MAP +
                " WHERE " + SEC_ACL_OBJ_PATH + " LIKE '/snapshotdefinitions/%'"
            );

            ResultSet rsAllSnapDfns = getAllSnapDfns.executeQuery();
            ResultSet rsAllSnapDfnSecObjProts = getAllSnapDfnSecObjProt.executeQuery();
            ResultSet rsAllSnapDfnAcls = getAllSnapDfnAcl.executeQuery();

            PreparedStatement deleteSecObjProt = con.prepareStatement(
                "DELETE FROM " + TBL_SEC_OBJ_PROT + " WHERE " + SEC_OBJ_PROT_PATH + " = ?"
            );
            PreparedStatement deleteSecAcl = con.prepareStatement(
                "DELETE FROM " + TBL_SEC_ACL_MAP + " WHERE " + SEC_ACL_OBJ_PATH + " = ?"
            );
        )
        {
            HashSet<SnapDfnKey> knownSnapDfns = new HashSet<>();
            while (rsAllSnapDfns.next())
            {
                knownSnapDfns.add(
                    new SnapDfnKey(
                        rsAllSnapDfns.getString(RD_RSC_NAME),
                        rsAllSnapDfns.getString(RD_SNAP_NAME)
                    )
                );
            }

            deleteOrphan(knownSnapDfns, asSet(rsAllSnapDfnAcls, SEC_ACL_OBJ_PATH), deleteSecAcl);
            deleteOrphan(knownSnapDfns, asSet(rsAllSnapDfnSecObjProts, SEC_OBJ_PROT_PATH), deleteSecObjProt);
        }
    }

    /**
     * Leftover: empty KVS (no props)
     */
    private void cleanupEmptyKvs(Connection con) throws SQLException
    {
        try (
            PreparedStatement selectAllKvs = con.prepareStatement("SELECT " + KVS_NAME + " FROM " + TBL_KVS);
            PreparedStatement selectAllPropInstances = con.prepareStatement(
                "SELECT DISTINCT(" + PROPS_INSTANCE + ")" +
                " FROM " + TBL_PROPS_CON +
                " WHERE " + PROPS_INSTANCE + " LIKE '/keyvaluestores/%'"
            );

            ResultSet allKvs = selectAllKvs.executeQuery();
            ResultSet allPropInstances = selectAllPropInstances.executeQuery();

            PreparedStatement delOrphanKvs = con.prepareStatement(
                "DELETE FROM " + TBL_KVS + " WHERE " + KVS_NAME + " = ?"
            );
        )
        {
            HashSet<String> kvsNameSet = asSet(allKvs, KVS_NAME);
            HashSet<String> propsInstanceSet = asSet(allPropInstances, PROPS_INSTANCE);

            HashSet<String> kvsToDelete = getKvsToDelete(kvsNameSet, propsInstanceSet);

            for (String kvsName : kvsToDelete)
            {
                delOrphanKvs.setString(1, kvsName);
                delOrphanKvs.execute();
            }
        }
    }

    public static HashSet<String> getKvsToDelete(Collection<String> kvsNameSet, Collection<String> propsInstanceSet)
    {
        final int indexOf = "/keyvaluestores/".length();

        HashSet<String> ret = new HashSet<>(kvsNameSet);
        for (String propInstance : propsInstanceSet)
        {
            ret.remove(propInstance.substring(indexOf));
        }
        return ret;
    }

    public static Set<String> getSecObjPathsToDelete(
        Collection<SnapDfnKey> knownSnapDfnsRef,
        Collection<String> allSnapDfnSecObjPathsRef
    )
    {
        HashSet<String> ret = new HashSet<>(allSnapDfnSecObjPathsRef);
        for (SnapDfnKey snapDfn : knownSnapDfnsRef)
        {
            ret.remove("/snapshotdefinitions/" + snapDfn.rscName + "/" + snapDfn.snapName);
        }
        return ret;
    }

    private void deleteOrphan(
        HashSet<SnapDfnKey> knownSnapDfns,
        HashSet<String> allSnapDfnSecObjPaths,
        PreparedStatement deletePrepStmt
    )
        throws SQLException
    {
        for (String objPath : getSecObjPathsToDelete(knownSnapDfns, allSnapDfnSecObjPaths))
        {
            deletePrepStmt.setString(1, objPath);
            deletePrepStmt.execute();
        }
    }

    private HashSet<String> asSet(ResultSet resultSet, String resultSetColumn) throws SQLException
    {
        HashSet<String> ret = new HashSet<>();
        while (resultSet.next())
        {
            ret.add(resultSet.getString(resultSetColumn));
        }
        return ret;
    }

    public static class SnapDfnKey
    {
        public final String rscName;
        public final String snapName;

        public SnapDfnKey(String rscNameRef, String snapNameRef)
        {
            rscName = rscNameRef;
            snapName = snapNameRef;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscName, snapName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof SnapDfnKey))
            {
                return false;
            }
            SnapDfnKey other = (SnapDfnKey) obj;
            return Objects.equals(rscName, other.rscName) &&
                Objects.equals(snapName, other.snapName);
        }
    }
}
