package com.linbit.drbdmanage.security;

import com.linbit.drbdmanage.ControllerDatabase;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Active role selection
 *
 * @author Robert Altnoeder <robert.altnoeder@linbit.com>
 */
public final class Authorization
{
    private ControllerDatabase ctrlDb;
    private DbAccessor dbDriver;

    public Authorization(AccessContext accCtx, ControllerDatabase ctrlDbRef, DbAccessor dbDriverRef)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        ctrlDb = ctrlDbRef;
        dbDriver = dbDriverRef;
    }

    public AccessContext assumeRole(AccessContext accCtx, Role reqRole)
        throws AccessDeniedException, FailedAuthorizationException
    {
        Connection dbConn = null;
        AccessContext reqCtx = null;
        try
        {
            dbConn = ctrlDb.getConnection();
            if (dbConn == null)
            {
                throw new SQLException(
                    "The controller database connection pool failed to provide a database connection"
                );
            }
            // Query the identity entry
            ResultSet idRoleEntry = dbDriver.getIdRoleMapEntry(dbConn, accCtx.subjectId.name, reqRole.name);
            if (idRoleEntry.next())
            {
                String idName = idRoleEntry.getString(SecurityDbFields.IDENTITY_NAME);
                String roleName = idRoleEntry.getString(SecurityDbFields.ROLE_NAME);
                // Double-check the entry
                if (accCtx.subjectId.name.value.equalsIgnoreCase(idName) &&
                    reqRole.name.value.equalsIgnoreCase(roleName))
                {
                    reqCtx = new AccessContext(accCtx.subjectId, reqRole, accCtx.subjectDomain, accCtx.privLimit);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new FailedAuthorizationException(
                "Role change failed: Database error: The SQL query for the security database record failed",
                // Description
                "Assuming the requested role failed due to a database error",
                // Cause
                "The database query for the security database record failed",
                // Correction
                "Contact a system administrator if the problem persists.\n" +
                "Review the error logged by the database to determine the cause of the problem.",
                // No error details
                null,
                sqlExc
            );
        }
        finally
        {
            ctrlDb.returnConnection(dbConn);
        }

        if (reqCtx == null)
        {
            throw new AccessDeniedException(
                "The Identity '" + accCtx.subjectId + "' is not authorized to assume the role '" + reqRole + "'.",
                "The request to assume the role '" + reqRole.name + "' was denied.",
                "The requesting identity '" + accCtx.subjectId + "' is not authorized to assume " +
                "the requested role.",
                "Authorization to use the role must be granted by an administrator.",
                null
            );
        }

        return reqCtx;
    }
}
