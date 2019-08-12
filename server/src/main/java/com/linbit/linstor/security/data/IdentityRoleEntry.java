package com.linbit.linstor.security.data;

public class IdentityRoleEntry
{
    private final String identiyName;
    private final String roleName;

    public IdentityRoleEntry(
        final String identiyNameRef,
        final String roleNameRef
    )
    {
        identiyName = identiyNameRef;
        roleName = roleNameRef;
    }

    public String getIdentiyName()
    {
        return identiyName;
    }

    public String getRoleName()
    {
        return roleName;
    }
}
