package com.linbit.drbdmanage.security;

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

    Role(RoleName roleName)
    {
        name = roleName;
        privileges = new PrivilegeSet();
    }
}
