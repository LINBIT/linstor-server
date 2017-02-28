package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.InvalidNameException;

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

    static
    {
        try
        {
            SYSTEM_ROLE = new Role(new RoleName("SYSTEM"));
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of the system role is invalid",
                nameExc
            );
        }
    }

    Role(RoleName roleName)
    {
        name = roleName;
        privileges = new PrivilegeSet();
    }
}
