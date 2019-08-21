package com.linbit.linstor.security.pojo;

public class IdentityRoleEntryPojo
{
    private final String identiyName;
    private final String roleName;

    public IdentityRoleEntryPojo(
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
