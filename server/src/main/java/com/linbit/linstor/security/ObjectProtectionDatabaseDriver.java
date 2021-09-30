package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

public interface ObjectProtectionDatabaseDriver
{
    void insertOp(ObjectProtection objProt) throws DatabaseException;

    void deleteOp(String objectPath) throws DatabaseException;

    void insertAcl(ObjectProtection parent, Role role, AccessType grantedAccess)
        throws DatabaseException;

    void updateAcl(ObjectProtection parent, Role role, AccessType grantedAccess)
        throws DatabaseException;

    void deleteAcl(ObjectProtection parent, Role role) throws DatabaseException;

    ObjectProtection loadObjectProtection(String objectPath, boolean logWarnIfNotExists)
        throws DatabaseException;

    SingleColumnDatabaseDriver<ObjectProtection, Identity> getIdentityDatabaseDrier();

    SingleColumnDatabaseDriver<ObjectProtection, Role> getRoleDatabaseDriver();

    SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver();

    default String getAclId(String objPath, String roleName, AccessType acType)
    {
        return "(ObjectPath=" + objPath + " Role=" + roleName + " AccessType=" + acType + ")";
    }

    default String getAclId(String objPath, String roleName)
    {
        return "(ObjectPath=" + objPath + " Role=" + roleName + ")";
    }

    default String getObjProtId(String objectPathRef)
    {
        return "(ObjProtPath=" + objectPathRef + ")";
    }
}
