package com.linbit.drbdmanage.security;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.DrbdSqlRuntimeException;


public class ObjectProtectionDerbyDriver implements ObjectProtectionDatabaseDriver
{
    private static final String TBL_OP      = SecurityDbFields.TBL_OBJ_PROT;
    private static final String TBL_ACL     = SecurityDbFields.TBL_ACL_MAP;
    private static final String TBL_ROLES   = SecurityDbFields.TBL_ROLES;

    private static final String OP_OBJECT_PATH      = SecurityDbFields.OBJECT_PATH;
    private static final String OP_CREATOR          = SecurityDbFields.CRT_IDENTITY_NAME;
    private static final String OP_OWNER            = SecurityDbFields.OWNER_ROLE_NAME;
    private static final String OP_SEC_TYPE_NAME    = SecurityDbFields.SEC_TYPE_NAME;

    private static final String ROLE_NAME           = SecurityDbFields.ROLE_NAME;
    private static final String ROLE_PRIVILEGES     = SecurityDbFields.ROLE_PRIVILEGES;

    private static final String ACL_OBJECT_PATH = SecurityDbFields.OBJECT_PATH;
    private static final String ACL_ROLE_NAME   = SecurityDbFields.ROLE_NAME;
    private static final String ACL_ACCESS_TYPE = SecurityDbFields.ACCESS_TYPE;

    // ObjectProtection SQL statements
    private static final String OP_INSERT =
        " INSERT INTO " + TBL_OP +
        " VALUES (?, ?, ?, ?)";
    private static final String OP_UPDATE =
        " UPDATE " + TBL_OP +
        " SET " + OP_CREATOR       + " = ?, " +
        "     " + OP_OWNER         + " = ?, " +
        "     " + OP_SEC_TYPE_NAME + " = ? " +
        " WHERE " + OP_OBJECT_PATH + " = ?";
    private static final String OP_UPDATE_IDENTITY =
        " UPDATE " + TBL_OP +
        " SET " + OP_CREATOR +       " = ? " +
        " WHERE " + OP_OBJECT_PATH + " = ?";
    private static final String OP_UPDATE_ROLE =
        " UPDATE " + TBL_OP +
        " SET " + OP_OWNER +         " = ? " +
        " WHERE " + OP_OBJECT_PATH + " = ?";
    private static final String OP_UPDATE_SEC_TYPE =
        " UPDATE " + TBL_OP +
        " SET " + OP_SEC_TYPE_NAME + " = ? " +
        " WHERE " + OP_OBJECT_PATH + " = ?";
    private static final String OP_DELETE =
        " DELETE FROM " + TBL_OP +
        " WHERE " + OP_OBJECT_PATH + " = ?";
    private static final String OP_LOAD =
        " SELECT " +
        "     OP." + OP_CREATOR + ", " +
        "     OP." + OP_OWNER + ", " +
        "     OP." + OP_SEC_TYPE_NAME + ", " +
        "     ROLE." + ROLE_PRIVILEGES +
        " FROM " +
        "     " + TBL_OP + " AS OP " +
        "     LEFT JOIN " + TBL_ROLES + " AS ROLE ON OP." + OP_OWNER + " = ROLE." + ROLE_NAME +
        " WHERE " +
        "     OP." + OP_OBJECT_PATH + " = ?";




    private static final String ACL_INSERT =
        " INSERT INTO " + TBL_ACL +
        " VALUES (?, ?, ?)";
    private static final String ACL_UPDATE =
        " UPDATE " + TBL_ACL +
        " SET " + ACL_ACCESS_TYPE + " = ? " +
        " WHERE " + ACL_OBJECT_PATH + " = ? AND " +
        "       " + ACL_ROLE_NAME +   " = ?";
    private static final String ACL_DELETE =
        " DELETE FROM " + TBL_ACL +
        " WHERE " + ACL_OBJECT_PATH + " = ? AND " +
        "       " + ACL_ROLE_NAME + " = ?";
    private static final String ACL_LOAD =
        " SELECT " +
        "     ACL." + ACL_ROLE_NAME + ", " +
        "     ACL." + ACL_ACCESS_TYPE +
        " FROM " +
        "     " + TBL_ACL + " AS ACL " +
        " WHERE " + ACL_OBJECT_PATH + " = ?";

    private SingleColumnDatabaseDriver<ObjectProtection, Identity> identityDriver;
    private SingleColumnDatabaseDriver<ObjectProtection, Role> roleDriver;
    private SingleColumnDatabaseDriver<ObjectProtection, SecurityType> securityTypeDriver;
    private AccessContext dbCtx;

    public ObjectProtectionDerbyDriver(AccessContext accCtx)
    {
        dbCtx = accCtx;
        identityDriver = new IdentityDerbyDriver();
        roleDriver = new RoleDerbyDriver();
        securityTypeDriver = new SecurityTypeDerbyDriver();
    }

    @Override
    public void insertOp(ObjectProtection objProt, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(OP_INSERT);

        stmt.setString(1, objProt.getObjectProtectionPath());
        stmt.setString(2, objProt.getCreator().name.value);
        stmt.setString(3, objProt.getOwner().name.value);
        stmt.setString(4, objProt.getSecurityType().name.value);

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public void updateOp(ObjectProtection objProt, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(OP_UPDATE);

        stmt.setString(1, objProt.getCreator().name.value);
        stmt.setString(2, objProt.getOwner().name.value);
        stmt.setString(3, objProt.getSecurityType().name.value);
        stmt.setString(4, objProt.getObjectProtectionPath());

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public void deleteOp(String objectPath, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(OP_DELETE);

        stmt.setString(1, objectPath);

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public void insertAcl(
        ObjectProtection parent,
        Role role,
        AccessType grantedAccess,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(ACL_INSERT);

        stmt.setString(1, parent.getObjectProtectionPath());
        stmt.setString(2, role.name.value);
        stmt.setLong(3, grantedAccess.getAccessMask());

        stmt.executeUpdate();
        stmt.close();

    }

    @Override
    public void updateAcl(
        ObjectProtection parent,
        Role role,
        AccessType grantedAccess,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(ACL_UPDATE);

        stmt.setLong(1, grantedAccess.getAccessMask());
        stmt.setString(2, parent.getObjectProtectionPath());
        stmt.setString(3, role.name.value);

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public void deleteAcl(ObjectProtection parent, Role role, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(ACL_DELETE);

        stmt.setString(1, parent.getObjectProtectionPath());
        stmt.setString(2, role.name.value);

        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public ObjectProtection loadObjectProtection(String objPath, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement opLoadStmt = transMgr.dbCon.prepareStatement(OP_LOAD);

        opLoadStmt.setString(1, objPath);

        ResultSet opResultSet = opLoadStmt.executeQuery();

        ObjectProtection objProt = null;
        if (opResultSet.next())
        {
            try
            {
                Identity identity = Identity.get(new IdentityName(opResultSet.getString(1)));
                Role role = Role.get(new RoleName(opResultSet.getString(2)));
                SecurityType secType = SecurityType.get(new SecTypeName(opResultSet.getString(3)));
                PrivilegeSet privLimitSet = new PrivilegeSet(opResultSet.getLong(4));
                AccessContext accCtx = new AccessContext(identity, role, secType, privLimitSet);
                objProt = new ObjectProtection(accCtx, objPath, this);
            }
            catch (InvalidNameException invalidNameExc)
            {
                opResultSet.close();
                opLoadStmt.close();
                throw new DrbdSqlRuntimeException(
                    "A name has been modified in the database to an illegal string.",
                    invalidNameExc
                );
            }
            opResultSet.close();
            opLoadStmt.close();
            // restore ACL
            PreparedStatement aclLoadStmt = transMgr.dbCon.prepareStatement(ACL_LOAD);
            aclLoadStmt.setString(1, objPath);
            ResultSet aclResultSet = aclLoadStmt.executeQuery();
            try
            {
                while (aclResultSet.next())
                {
                    Role role = Role.get(new RoleName(aclResultSet.getString(1)));
                    AccessType type = AccessType.get(aclResultSet.getInt(2));

                    objProt.addAclEntry(dbCtx, role, type);
                }
                aclResultSet.close();
                aclLoadStmt.close();
            }
            catch (InvalidNameException invalidNameExc)
            {
                aclLoadStmt.close();
                aclResultSet.close();
                throw new DrbdSqlRuntimeException(
                    "A name has been modified in the database to an illegal string.",
                    invalidNameExc
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                aclLoadStmt.close();
                aclResultSet.close();
                throw new ImplementationError(
                    " Database's accessContext has insufficient rights to restore object protection",
                    accessDeniedExc
                );
            }
        }
        else
        {
            // TODO: log warning that op not found
        }
        opResultSet.close();
        opLoadStmt.close();

        return objProt;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier()
    {
        return identityDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver()
    {
        return roleDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
    {
        return securityTypeDriver;
    }

    private class IdentityDerbyDriver implements SingleColumnDatabaseDriver<ObjectProtection, Identity>
    {
        @Override
        public void update(ObjectProtection parent, Identity element, TransactionMgr transMgr) throws SQLException
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(OP_UPDATE_IDENTITY);

            stmt.setString(1, element.name.value);
            stmt.setString(2, parent.getObjectProtectionPath());

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class RoleDerbyDriver implements SingleColumnDatabaseDriver<ObjectProtection, Role>
    {
        @Override
        public void update(ObjectProtection parent, Role element, TransactionMgr transMgr) throws SQLException
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(OP_UPDATE_ROLE);

            stmt.setString(1, element.name.value);
            stmt.setString(2, parent.getObjectProtectionPath());

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class SecurityTypeDerbyDriver implements SingleColumnDatabaseDriver<ObjectProtection, SecurityType>
    {
        @Override
        public void update(ObjectProtection parent, SecurityType element, TransactionMgr transMgr) throws SQLException
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(OP_UPDATE_SEC_TYPE);

            stmt.setString(1, element.name.value);
            stmt.setString(2, parent.getObjectProtectionPath());

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
