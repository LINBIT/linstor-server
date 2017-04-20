package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Represents the type of an object protected by access controls
 * or the domain of a subject
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class SecurityType
{
    // Name of this security type
    public final SecTypeName name;

    // Access control rules for this type
    private final Map<SecTypeName, AccessType> rules;

    static final SecurityType SYSTEM_TYPE;
    static final SecurityType PUBLIC_TYPE;

    static
    {
        try
        {
            SYSTEM_TYPE = new SecurityType(new SecTypeName("SYSTEM"));
            PUBLIC_TYPE = new SecurityType(new SecTypeName("PUBLIC"));
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of a builtin security type/domain is invalid",
                nameExc
            );
        }
    }

    public SecurityType(AccessContext accCtx, SecTypeName typeName)
        throws AccessDeniedException
    {
        this(typeName);
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
    }

    SecurityType(SecTypeName typeName)
    {
        name = typeName;
        rules = new TreeMap<>();
    }

    /**
     * Checks whether the subject domain has the requested type of access
     * to objects with the security type of this instance
     *
     * @param context The security context of the subject requesting access
     * @param requested The type of access requested by the subject
     * @throws AccessDeniedException If access is denied
     */
    public final void requireAccess(AccessContext context, AccessType requested)
        throws AccessDeniedException
    {
        SecurityLevel globalSecLevel = SecurityLevel.get();
        switch (globalSecLevel)
        {
            case NO_SECURITY:
                break;
            case RBAC:
                break;
            case MAC:
                {
                    boolean allowFlag = false;

                    // Get the name of the subject's security domain
                    SecTypeName typeName = context.subjectDomain.name;

                    // Look for a rule allowing a certain type of access
                    // between from the subject's security domain to this
                    // security type
                    AccessType accType = rules.get(typeName);

                    // If a rule entry was found, check whether the requested type
                    // of access is within the bounds of the type of access
                    // allowed by the rule entry
                    if (accType != null)
                    {
                        allowFlag = accType.hasAccess(requested);
                    }

                    // Allow access if the current role has MAC_OVRD privileges
                    if (!allowFlag)
                    {
                        PrivilegeSet privileges = context.subjectRole.privileges;
                        allowFlag = privileges.hasPrivileges(Privilege.PRIV_MAC_OVRD);
                    }

                    if (!allowFlag)
                    {
                        throw new AccessDeniedException(
                            "Access of type '" + requested +
                            "' not allowed by mandatory access control rules"
                        );
                    }
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
     * Returns the level of access granted to an object of the security type of
     * this instance by an access control rule
     * @param context Security context specifying the subject domain
     * @return Allowed level of access, or null if access is denied
     */
    public final AccessType queryAccess(AccessContext context)
    {
        SecurityType domain = context.subjectDomain;
        return queryAccess(domain);
    }

    /**
     * Returns the level of access granted to an object of the security type
     * of this instance to the specified security domain by an access control rule
     * @param domain The security domain to find an access control rule for
     * @return Allowed level of access, or null if access is denied
     */
    public final AccessType queryAccess(SecurityType domain)
    {
        return rules.get(domain.name);
    }

    public final void addEntry(AccessContext context, SecurityType domain, AccessType grantedAccess)
        throws AccessDeniedException
    {
        PrivilegeSet privs = context.getEffectivePrivs();
        privs.requirePrivileges(Privilege.PRIV_SYS_ALL);
        rules.put(domain.name, grantedAccess);
    }

    public final void delEntry(AccessContext context, SecurityType domain)
        throws AccessDeniedException
    {
        PrivilegeSet privs = context.getEffectivePrivs();
        privs.requirePrivileges(Privilege.PRIV_SYS_ALL);
        rules.remove(domain.name);
    }

    @Override
    public final String toString()
    {
        return name.displayValue;
    }
}
