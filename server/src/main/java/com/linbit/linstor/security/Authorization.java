package com.linbit.linstor.security;

import com.linbit.ErrorCheck;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.pojo.IdentityRoleEntryPojo;

public class Authorization
{
    private ControllerDatabase ctrlDb;
    private DbAccessor dbDriver;

    public Authorization(AccessContext accCtx, ControllerDatabase ctrlDbRef, DbAccessor dbDriverRef)
        throws AccessDeniedException
    {
        ErrorCheck.ctorNotNull(Authentication.class, AccessContext.class, accCtx);
        ErrorCheck.ctorNotNull(Authentication.class, ControllerDatabase.class, ctrlDbRef);
        ErrorCheck.ctorNotNull(Authentication.class, DbAccessor.class, dbDriverRef);

        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        ctrlDb = ctrlDbRef;
        dbDriver = dbDriverRef;
    }

    public AccessContext assumeRole(AccessContext accCtx, Role reqRole)
        throws AccessDeniedException, FailedAuthorizationException
    {
        AccessContext reqCtx = null;
        try
        {
            // Query the identity entry
            IdentityRoleEntryPojo idRoleEntry = dbDriver.getIdRoleMapEntry(ctrlDb, accCtx.subjectId.name, reqRole.name);
            if (idRoleEntry != null)
            {
                // Double-check the entry
                if (accCtx.subjectId.name.value.equalsIgnoreCase(idRoleEntry.getIdentiyName()) &&
                    reqRole.name.value.equalsIgnoreCase(idRoleEntry.getRoleName()))
                {
                    reqCtx = new AccessContext(accCtx.subjectId, reqRole, accCtx.subjectDomain, accCtx.privLimit);
                }
            }
        }
        catch (DatabaseException dbExc)
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
                dbExc
            );
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
