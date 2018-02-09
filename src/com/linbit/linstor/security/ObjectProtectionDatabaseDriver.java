package com.linbit.linstor.security;

import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;

public interface ObjectProtectionDatabaseDriver
{
    void insertOp(ObjectProtection objProt, TransactionMgr transMgr) throws SQLException;

    void deleteOp(String objectPath, TransactionMgr transMgr) throws SQLException;

    void insertAcl(
        ObjectProtection parent,
        Role role,
        AccessType grantedAccess,
        TransactionMgr transMgr
    )
        throws SQLException;

    void updateAcl(
        ObjectProtection parent,
        Role role,
        AccessType grantedAccess,
        TransactionMgr transMgr
    )
        throws SQLException;

    void deleteAcl(ObjectProtection parent, Role role, TransactionMgr transMgr) throws SQLException;

    ObjectProtection loadObjectProtection(
        String objectPath,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier();

    SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver();

    SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver();
}
