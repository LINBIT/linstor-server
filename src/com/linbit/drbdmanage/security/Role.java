package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;

/**
 * Security role
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Role
{
    // Name of this security role
    public final RoleName name;

    // Set of privileges assigned to this role
    public final PrivilegeSet privileges;

    static final Role SYSTEM_ROLE;
    static final Role PUBLIC_ROLE;

    static
    {
        try
        {
            SYSTEM_ROLE = new Role(new RoleName("SYSTEM"));
            PUBLIC_ROLE = new Role(new RoleName("PUBLIC"));
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of a builtin role is invalid",
                nameExc
            );
        }
    }

    Role(RoleName roleName)
    {
        name = roleName;
        privileges = new PrivilegeSet();
    }

    public Role(AccessContext accCtx, RoleName roleName)
        throws AccessDeniedException
    {
        this(roleName);
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
    }

    @Override
    public final String toString()
    {
        return name.displayValue;
    }
}
