package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;

public class EmptySecurityDbDriver implements DbAccessor
{

    public static final SingleColumnDatabaseDriver<?, ?> NOOP_COL_DRIVER = new NoOpColDriver();

    @Override
    public ResultSet getSignInEntry(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getIdRoleMapEntry(Connection dbConn, IdentityName idName, RoleName rlName) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getDefaultRole(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadIdentities(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadSecurityTypes(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadRoles(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadTeRules(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet loadSecurityLevel(Connection dbConn) throws SQLException
    {
        return null;
    }

    @Override
    public ObjectProtectionDatabaseDriver getObjectProtectionDatabaseDriver()
    {
        return new EmptyObjectProtectionDatabaseDriver();
    }

    private static class EmptyObjectProtectionDatabaseDriver implements ObjectProtectionDatabaseDriver
    {
        public EmptyObjectProtectionDatabaseDriver()
        {
        }

        @Override
        public void insertOp(ObjectProtection objProt, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void deleteOp(String objPath, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void insertAcl(
            ObjectProtection objProt,
            Role role,
            AccessType grantedAccess,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            // no-op
        }

        @Override
        public void updateAcl(
            ObjectProtection objProt,
            Role role,
            AccessType grantedAccess,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            // no-op
        }

        @Override
        public void deleteAcl(ObjectProtection objProt, Role role, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public ObjectProtection loadObjectProtection(
            String objPath,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            // no-op
            return null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier()
        {
            // no-op
            return (SingleColumnDatabaseDriver<ObjectProtection, Identity>) NOOP_COL_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver()
        {
            // no-op
            return (SingleColumnDatabaseDriver<ObjectProtection, Role>) NOOP_COL_DRIVER;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
        {
            // no-op
            return (SingleColumnDatabaseDriver<ObjectProtection, SecurityType>) NOOP_COL_DRIVER;
        }
    }

    private static class NoOpColDriver implements SingleColumnDatabaseDriver<Object, Object>
    {

        @Override
        public void update(Object parent, Object element, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

    }
}
