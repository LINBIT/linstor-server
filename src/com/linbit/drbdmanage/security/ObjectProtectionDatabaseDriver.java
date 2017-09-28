package com.linbit.drbdmanage.security;

import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;

public interface ObjectProtectionDatabaseDriver
{
    public void insertOp(ObjectProtection objProt, TransactionMgr transMgr) throws SQLException;

    public void deleteOp(String objectPath, TransactionMgr transMgr) throws SQLException;

    public void insertAcl(
        ObjectProtection parent,
        Role role,
        AccessType grantedAccess,
        TransactionMgr transMgr
    )
        throws SQLException;

    public void updateAcl(
        ObjectProtection parent,
        Role role,
        AccessType grantedAccess,
        TransactionMgr transMgr
    )
        throws SQLException;

    public void deleteAcl(ObjectProtection parent, Role role, TransactionMgr transMgr) throws SQLException;

    public ObjectProtection loadObjectProtection(
        String objectPath,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    public SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier();

    public SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver();

    public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver();
}
