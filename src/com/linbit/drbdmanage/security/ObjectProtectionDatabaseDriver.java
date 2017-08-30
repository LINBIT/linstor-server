package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;

public interface ObjectProtectionDatabaseDriver
{
    void insertOp(Connection con, ObjectProtection objProt) throws SQLException;

    void updateOp(Connection con, ObjectProtection objProt) throws SQLException;

    void deleteOp(Connection con) throws SQLException;

    void insertAcl(Connection con, Role role, AccessType grantedAccess) throws SQLException;

    void updateAcl(Connection con, Role role, AccessType grantedAccess) throws SQLException;

    void deleteAcl(Connection con, Role role) throws SQLException;

    ObjectProtection loadObjectProtection(Connection con) throws SQLException;

    SingleColumnDatabaseDriver<Identity> getIdentityDatabaseDrier();

    SingleColumnDatabaseDriver<Role> getRoleDatabaseDriver();

    SingleColumnDatabaseDriver<SecurityType> getSecurityTypeDriver();
}
