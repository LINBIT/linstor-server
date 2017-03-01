package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import java.util.Map;
import java.util.TreeMap;

/**
 * Object access control list
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class AccessControlList
{
    private final Map<RoleName, AccessControlEntry> acl;

    AccessControlList()
    {
        acl = new TreeMap<>();
    }

    /**
     * Checks whether the subject has the requested type of access
     * to objects protected by this access control list instance
     *
     * @param context The security context of the subject requesting access
     * @param requested The type of access requested by the subject
     * @throws AccessDeniedException If access is denied
     */
    public final void requireAccess(AccessContext context, AccessType requested)
        throws AccessDeniedException
    {
        boolean allowFlag = false;

        // Look for an entry for the subject's role in this access control list
        AccessControlEntry entry = acl.get(context.subjectRole.name);

        // If an entry was found, check whether the requested level of access
        // is within the bounds of the level of access allowed by the
        // access control entry.
        // If no entry was found, access is denied.
        if (entry != null)
        {
            allowFlag = entry.access.hasAccess(requested);
        }

        if (!allowFlag)
        {
            allowFlag |= hasAccessPrivilege(context, requested);
        }

        if (!allowFlag)
        {
            throw new AccessDeniedException(
                "Access of type '" + requested + "' not allowed by the access control list"
            );
        }
    }

    /**
     * Returns the level of access that is granted by the access control list
     * to the role referenced by the specified security context
     *
     * @param context Security context for access controls
     * @return Allowed level of access, or null if access is denied
     */
    public final AccessType queryAccess(AccessContext context)
    {
        Role subjRole = context.subjectRole;
        return queryAccess(subjRole);
    }

    /**
     * Returns the level of access that is granted by the access control list
     * to the specified role
     *
     * @param subjRole The role to find access control entries for
     * @return Allowed level of access, or null if access is denied
     */
    public final AccessType queryAccess(Role subjRole)
    {
        AccessType access = null;
        AccessControlEntry entry = acl.get(subjRole.name);
        if (entry != null)
        {
            access = entry.access;
        }
        return access;
    }

    private boolean hasAccessPrivilege(AccessContext context, AccessType requested)
    {
        PrivilegeSet privileges = context.subjectRole.privileges;

        boolean allowFlag = false;
        // Higher-level access privileges include the lower-level access privileges,
        // therefore, no extra check is required
        // E.g., if an OBJ_CHANGE privilege is present, the check for OBJ_VIEW
        // yields true
        switch (requested)
        {
            case VIEW:
                allowFlag |= privileges.hasPrivileges(Privilege.PRIV_OBJ_VIEW);
                break;
            case USE:
                allowFlag |= privileges.hasPrivileges(Privilege.PRIV_OBJ_USE);
                break;
            case CHANGE:
                allowFlag |= privileges.hasPrivileges(Privilege.PRIV_OBJ_CHANGE);
                break;
            case CONTROL:
                allowFlag |= privileges.hasPrivileges(Privilege.PRIV_OBJ_CONTROL);
                break;
            default:
                throw new ImplementationError(
                    "Switch statement reached default case, unhandled enumeration case",
                    null
                );
        }
        return allowFlag;
    }
}
