package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Singleton
public class ObjectProtectionGenericDbDriver implements ObjectProtectionDatabaseDriver
{
    private static final String TBL_OP      = DbConstants.TBL_SEC_OBJECT_PROTECTION;
    private static final String TBL_ACL     = DbConstants.TBL_SEC_ACL_MAP;
    private static final String TBL_ROLES   = DbConstants.TBL_SEC_ROLES;

    private static final String OP_OBJECT_PATH      = DbConstants.OBJECT_PATH;
    private static final String OP_CREATOR          = DbConstants.CREATOR_IDENTITY_NAME;
    private static final String OP_OWNER            = DbConstants.OWNER_ROLE_NAME;
    private static final String OP_SEC_TYPE_NAME    = DbConstants.SECURITY_TYPE_NAME;

    private static final String ROLE_NAME           = DbConstants.ROLE_NAME;
    private static final String ROLE_PRIVILEGES     = DbConstants.ROLE_PRIVILEGES;

    private static final String ACL_OBJECT_PATH = DbConstants.OBJECT_PATH;
    private static final String ACL_ROLE_NAME   = DbConstants.ROLE_NAME;
    private static final String ACL_ACCESS_TYPE = DbConstants.ACCESS_TYPE;

    // ObjectProtection SQL statements
    private static final String OP_INSERT =
        " INSERT INTO " + TBL_OP +
        " (" +
            OP_OBJECT_PATH + ", " + OP_CREATOR + ", " +
            OP_OWNER + ", " + OP_SEC_TYPE_NAME +
        ") VALUES (?, ?, ?, ?)";
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
        " (" + ACL_OBJECT_PATH + ", " + ACL_ROLE_NAME + ", " + ACL_ACCESS_TYPE + ")" +
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
    private static final String ACL_DELETE_ALL =
        " DELETE FROM " + TBL_ACL + " WHERE " + ACL_OBJECT_PATH + " = ?";

    private SingleColumnDatabaseDriver<ObjectProtection, Identity> identityDriver;
    private SingleColumnDatabaseDriver<ObjectProtection, Role> roleDriver;
    private SingleColumnDatabaseDriver<ObjectProtection, SecurityType> securityTypeDriver;
    private AccessContext dbCtx;
    private ErrorReporter errorReporter;
    private TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ObjectProtectionGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        transMgrProvider = transMgrProviderRef;
        identityDriver = new IdentityGenericDbDriver();
        roleDriver = new RoleGenericDbDriver();
        securityTypeDriver = new SecurityTypeGenericDbDriver();
        errorReporter = errorReporterRef;
        transObjFactory = transObjFactoryRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void insertOp(ObjectProtection objProt) throws DatabaseException
    {
        errorReporter.logTrace("Creating ObjectProtection %s", getObjProtId(objProt.getObjectProtectionPath()));
        try (PreparedStatement stmt = getConnection().prepareStatement(OP_INSERT))
        {
            stmt.setString(1, objProt.getObjectProtectionPath());
            stmt.setString(2, objProt.getCreator().name.value);
            stmt.setString(3, objProt.getOwner().name.value);
            stmt.setString(4, objProt.getSecurityType().name.value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("ObjectProtection created %s", getObjProtId(objProt.getObjectProtectionPath()));
    }

    @Override
    public void deleteOp(String objectPath) throws DatabaseException
    {
        errorReporter.logTrace("Deleting ObjectProtection %s", getObjProtId(objectPath));
        try (PreparedStatement stmt = getConnection().prepareStatement(OP_DELETE))
        {
            stmt.setString(1, objectPath);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        try (PreparedStatement stmt = getConnection().prepareStatement(ACL_DELETE_ALL))
        {
            stmt.setString(1, objectPath);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }

        errorReporter.logTrace("ObjectProtection deleted %s", getObjProtId(objectPath));
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void insertAcl(ObjectProtection parent, Role role, AccessType grantedAccess)
        throws DatabaseException
    {
        errorReporter.logTrace("Creating AccessControl entry %s", getAclTraceId(parent, role, grantedAccess));
        try (PreparedStatement stmt = getConnection().prepareStatement(ACL_INSERT))
        {
            stmt.setString(1, parent.getObjectProtectionPath());
            stmt.setString(2, role.name.value);
            stmt.setLong(3, grantedAccess.getAccessMask());

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("AccessControl entry created %s", getAclDebugId(parent, role, grantedAccess));
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void updateAcl(ObjectProtection parent, Role role, AccessType grantedAccess)
        throws DatabaseException
    {
        errorReporter.logTrace(
            "Updating AccessControl entry from %s to %s %s",
            parent.getAcl().getEntry(role),
            grantedAccess,
            getAclTraceId(parent, role)
        );
        try (PreparedStatement stmt = getConnection().prepareStatement(ACL_UPDATE))
        {
            stmt.setLong(1, grantedAccess.getAccessMask());
            stmt.setString(2, parent.getObjectProtectionPath());
            stmt.setString(3, role.name.value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace(
            "AccessControl entry updated from %s to %s %s",
            parent.getAcl().getEntry(role),
            grantedAccess,
            getAclDebugId(parent, role)
        );
    }

    @Override
    public void deleteAcl(ObjectProtection parent, Role role) throws DatabaseException
    {
        errorReporter.logTrace("Deleting AccessControl entry %s", getAclTraceId(parent, role));
        try (PreparedStatement stmt = getConnection().prepareStatement(ACL_DELETE))
        {
            stmt.setString(1, parent.getObjectProtectionPath());
            stmt.setString(2, role.name.value);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("AccessControl entry deleted %s", getAclDebugId(parent, role));
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public ObjectProtection loadObjectProtection(String objPath, boolean logWarnIfNotExists)
        throws DatabaseException
    {
        errorReporter.logTrace("Loading ObjectProtection %s", getObjProtId(objPath));
        ObjectProtection objProt = null;

        try (PreparedStatement opLoadStmt = getConnection().prepareStatement(OP_LOAD))
        {
            opLoadStmt.setString(1, objPath);

            try (ResultSet opResultSet = opLoadStmt.executeQuery())
            {
                if (opResultSet.next())
                {
                    Identity identity = null;
                    Role role = null;
                    SecurityType secType = null;
                    try
                    {
                        identity = Identity.get(new IdentityName(opResultSet.getString(1)));
                        role = Role.get(new RoleName(opResultSet.getString(2)));
                        secType = SecurityType.get(new SecTypeName(opResultSet.getString(3)));
                        PrivilegeSet privLimitSet = new PrivilegeSet(opResultSet.getLong(4));
                        AccessContext accCtx = new AccessContext(identity, role, secType, privLimitSet);
                        objProt = new ObjectProtection(accCtx, objPath, this, transObjFactory, transMgrProvider);
                        objProt.setPersisted(true);
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        String name;
                        String invalidValue;
                        if (identity == null)
                        {
                            name = "IdentityName";
                            invalidValue = opResultSet.getString(1);
                        }
                        else
                        if (role == null)
                        {
                            name = "RoleName";
                            invalidValue = opResultSet.getString(2);
                        }
                        else
                        {
                            name = "SecTypeName";
                            invalidValue = opResultSet.getString(3);
                        }
                        throw new LinStorDBRuntimeException(
                            String.format(
                                "A stored %s in the table %s could not be restored." +
                                    "(ObjectPath=%s, invalid %s=%s)",
                                name,
                                TBL_OP,
                                objPath,
                                name,
                                invalidValue
                            ),
                            invalidNameExc
                        );
                    }
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        if (objProt != null)
        {
            errorReporter.logTrace("ObjectProtection instance created. %s", getObjProtId(objPath));
            // restore ACL

            try (PreparedStatement aclLoadStmt = getConnection().prepareStatement(ACL_LOAD))
            {
                aclLoadStmt.setString(1, objPath);
                String currentRoleName = null;
                try (ResultSet aclResultSet = aclLoadStmt.executeQuery())
                {
                    while (aclResultSet.next())
                    {
                        currentRoleName = aclResultSet.getString(1);
                        Role role = Role.get(new RoleName(currentRoleName));
                        AccessType type = AccessType.get(aclResultSet.getInt(2));

                        objProt.restoreAclEntry(role, type);
                    }
                }
                catch (InvalidNameException invalidNameExc)
                {
                    throw new LinStorDBRuntimeException(
                        String.format(
                            "A stored RoleName in the table %s could not be restored." +
                                "(ObjectPath=%s, invalid RoleName=%s)",
                            TBL_OP,
                            objPath,
                            currentRoleName
                        ),
                        invalidNameExc
                    );
                }
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace("AccessControl entries restored %s", getObjProtId(objPath));

            errorReporter.logTrace("ObjectProtection loaded %s", getObjProtId(objPath));
        }
        else
        if (logWarnIfNotExists)
        {
            errorReporter.logWarning("ObjectProtection not found in DB %s", getObjProtId(objPath));
        }
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

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getAclTraceId(ObjectProtection parent, Role role, AccessType grantedAccess)
    {
        return getAclId(
            parent.getObjectProtectionPath(),
            role.name.value,
            grantedAccess
        );
    }

    private String getAclDebugId(ObjectProtection parent, Role role, AccessType grantedAccess)
    {
        return getAclId(
            parent.getObjectProtectionPath(),
            role.name.displayValue,
            grantedAccess
        );
    }

    private String getAclTraceId(ObjectProtection parent, Role role)
    {
        return getAclId(
            parent.getObjectProtectionPath(),
            role.name.value
        );
    }

    private String getAclDebugId(ObjectProtection parent, Role role)
    {
        return getAclId(
            parent.getObjectProtectionPath(),
            role.name.displayValue
        );
    }

    private String getAclId(String objPath, String roleName, AccessType acType)
    {
        return "(ObjectPath=" + objPath + " Role=" + roleName + " AccessType=" + acType + ")";
    }

    private String getAclId(String objPath, String roleName)
    {
        return "(ObjectPath=" + objPath + " Role=" + roleName + ")";
    }

    private String getObjProtId(String objectProtectionPath)
    {
        return "(ObjProtPath=" + objectProtectionPath + ")";
    }

    private class IdentityGenericDbDriver implements SingleColumnDatabaseDriver<ObjectProtection, Identity>
    {
        @Override
        public void update(ObjectProtection parent, Identity creator) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating ObjectProtection's Creator from %s to %s. %s",
                parent.getCreator().name.value,
                creator.name.value,
                getObjProtId(parent.getObjectProtectionPath())
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(OP_UPDATE_IDENTITY))
            {
                stmt.setString(1, creator.name.value);
                stmt.setString(2, parent.getObjectProtectionPath());

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "ObjectProtection's Creator updated from %s to %s. %s",
                parent.getCreator().name.displayValue,
                creator.name.displayValue,
                getObjProtId(parent.getObjectProtectionPath())
            );
        }
    }

    private class RoleGenericDbDriver implements SingleColumnDatabaseDriver<ObjectProtection, Role>
    {
        @Override
        public void update(ObjectProtection parent, Role owner) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating ObjectProtection's Owner from %s to %s. %s",
                parent.getOwner().name.value,
                owner.name.value,
                getObjProtId(parent.getObjectProtectionPath())
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(OP_UPDATE_ROLE))
            {
                stmt.setString(1, owner.name.value);
                stmt.setString(2, parent.getObjectProtectionPath());

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "ObjectProtection's Creator updated from %s to %s. %s",
                parent.getCreator().name.displayValue,
                owner.name.displayValue,
                getObjProtId(parent.getObjectProtectionPath())
            );
        }
    }

    private class SecurityTypeGenericDbDriver implements SingleColumnDatabaseDriver<ObjectProtection, SecurityType>
    {
        @Override
        public void update(ObjectProtection parent, SecurityType secType) throws DatabaseException
        {
            errorReporter.logTrace(
                "Updating ObjectProtection's SecurityType from %s to %s. %s",
                parent.getSecurityType().name.value,
                secType.name.value,
                getObjProtId(parent.getObjectProtectionPath())
            );
            try (PreparedStatement stmt = getConnection().prepareStatement(OP_UPDATE_SEC_TYPE))
            {
                stmt.setString(1, secType.name.value);
                stmt.setString(2, parent.getObjectProtectionPath());

                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
            errorReporter.logTrace(
                "ObjectProtection's SecurityType updated from %s to %s. %s",
                parent.getCreator().name.displayValue,
                secType.name.displayValue,
                getObjProtId(parent.getObjectProtectionPath())
            );
        }
    }
}
