package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import java.util.Collections;
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
        acl = Collections.synchronizedMap(new TreeMap<RoleName, AccessControlEntry>());
    }

    /**
     * Checks whether the subject has the requested type of access
     * to objects protected by this access control list instance
     *
     * @param context The security context of the subject requesting access
     * @param requested The type of access requested by the subject
     * @throws AccessDeniedException If access is denied
     */
    public void requireAccess(AccessContext context, AccessType requested)
        throws AccessDeniedException
    {
        SecurityLevel globalSecLevel = SecurityLevel.get();
        switch (globalSecLevel)
        {
            case NO_SECURITY:
                break;
            case RBAC:
                // fall-through
            case MAC:
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
                        "Access of type '" + requested + "' not allowed by the " +
                        "access control list",
                        // Description
                        "Access to the protected object was denied",
                        // Cause
                        "The access control list for the protected object does not allow " +
                        "access of type " + requested.name() + " by role " +
                        context.subjectRole.name,
                        // Correction
                        "An entry that allows access must be added by an authorized role",
                        // No error details
                        null
                    );
                }
                break;
            default:
                throw new ImplementationError(
                    "Missing case label for enum constant " + globalSecLevel.name(),
                    null
                );
        }
    }

    /**
     * Returns the level of access to the object protected by this access control list instance
     * that is granted to the specified security context
     *
     * @param context The security context of the subject requesting access
     * @return Allowed AccessType, or null if access is denied
     */
    public AccessType queryAccess(AccessContext context)
    {
        AccessType result = null;
        SecurityLevel globalSecLevel = SecurityLevel.get();
        switch (globalSecLevel)
        {
            case NO_SECURITY:
                result = AccessType.CONTROL;
                break;
            case RBAC:
                // fall-through
            case MAC:
                // Query the level of access allowed by privileges
                AccessType privAccess = context.privEffective.toRbacAccess();

                // Look for an entry for the subject's role in this access control list
                AccessType aclAccess = null;
                {
                    AccessControlEntry entry = acl.get(context.subjectRole.name);
                    if (entry != null)
                    {
                        aclAccess = entry.access;
                    }
                }

                // Combine access permissions
                result = AccessType.union(privAccess, aclAccess);
                break;
            default:
                throw new AssertionError(globalSecLevel.name());
        }
        return result;
    }

    /**
     * Returns the level of access that is granted by the access control list
     * to the role referenced by the specified security context
     *
     * @param context Security context for access controls
     * @return Allowed level of access, or null if access is denied
     */
    public AccessType getEntry(AccessContext context)
    {
        Role subjRole = context.subjectRole;
        return getEntry(subjRole);
    }

    /**
     * Returns the level of access that is granted by the access control list
     * to the specified role
     *
     * @param subjRole The role to find access control entries for
     * @return Allowed level of access, or null if access is denied
     */
    public AccessType getEntry(Role subjRole)
    {
        AccessType access = null;
        AccessControlEntry entry = acl.get(subjRole.name);
        if (entry != null)
        {
            access = entry.access;
        }
        return access;
    }

    void addEntry(Role entryRole, AccessType grantedAccess)
    {
        AccessControlEntry entry = new AccessControlEntry(entryRole, grantedAccess);
        acl.put(entryRole.name, entry);
    }

    void delEntry(Role entryRole)
    {
        acl.remove(entryRole.name);
    }

    private boolean hasAccessPrivilege(AccessContext context, AccessType requested)
    {
        PrivilegeSet privileges = context.privEffective;

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
