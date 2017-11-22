package com.linbit.linstor.security;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;

public class NoOpSecurityDriver implements DbAccessor
{
    private final SingleColumnDatabaseDriver<?, ?> NoOpSingleColDriver = new NoOpSingleColDriver();

    private final ObjectProtectionDatabaseDriver NoOpObjProtDriver = new NoOpObjectProtectionDriver();

    private final ObjectProtection dummyObjProt;

    public NoOpSecurityDriver(AccessContext privCtx)
    {
        dummyObjProt = new ObjectProtection(privCtx, "", NoOpObjProtDriver);
    }

    @Override
    public ResultSet getSignInEntry(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet getIdRoleMapEntry(Connection dbConn, IdentityName idName, RoleName rlName) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet getDefaultRole(Connection dbConn, IdentityName idName) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadIdentities(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadSecurityTypes(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadRoles(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadTeRules(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ResultSet loadSecurityLevel(Connection dbConn) throws SQLException
    {
        return null; // intentionally null
    }

    @Override
    public ObjectProtectionDatabaseDriver getObjectProtectionDatabaseDriver()
    {
        return NoOpObjProtDriver;
    }

    @Override
    public void setSecurityLevel(Connection dbConn, SecurityLevel newLevel) throws SQLException
    {
        // no-op
    }

    private class NoOpObjectProtectionDriver implements ObjectProtectionDatabaseDriver
    {

        @Override
        public void insertOp(ObjectProtection objProt, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void deleteOp(String objectPath, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public void insertAcl(ObjectProtection parent, Role role, AccessType grantedAccess, TransactionMgr transMgr)
            throws SQLException
        {
            // no-op
        }

        @Override
        public void updateAcl(ObjectProtection parent, Role role, AccessType grantedAccess, TransactionMgr transMgr)
            throws SQLException
        {
            // no-op
        }

        @Override
        public void deleteAcl(ObjectProtection parent, Role role, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }

        @Override
        public ObjectProtection loadObjectProtection(
            String objectPath,
            boolean logWarnIfNotExists,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            return dummyObjProt;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier()
        {
            return (SingleColumnDatabaseDriver<ObjectProtection, Identity>) NoOpSingleColDriver;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver()
        {
            return (SingleColumnDatabaseDriver<ObjectProtection, Role>) NoOpSingleColDriver;
        }

        @SuppressWarnings("unchecked")
        @Override
        public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
        {
            return (SingleColumnDatabaseDriver<ObjectProtection, SecurityType>) NoOpSingleColDriver;
        }
    }

    private static class NoOpSingleColDriver implements SingleColumnDatabaseDriver<Object, Object>
    {
        @Override
        public void update(Object parent, Object element, TransactionMgr transMgr) throws SQLException
        {
            // no-op
        }
    }
}
