package com.linbit.linstor.security;

import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;

public interface ObjectProtectionDatabaseDriver
{
    void insertOp(ObjectProtection objProt) throws SQLException;

    void deleteOp(String objectPath) throws SQLException;

    void insertAcl(ObjectProtection parent, Role role, AccessType grantedAccess)
        throws SQLException;

    void updateAcl(ObjectProtection parent, Role role, AccessType grantedAccess)
        throws SQLException;

    void deleteAcl(ObjectProtection parent, Role role) throws SQLException;

    ObjectProtection loadObjectProtection(String objectPath, boolean logWarnIfNotExists)
        throws SQLException;

    SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier();

    SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver();

    SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver();
}
