package com.linbit.linstor.security.pojo;

import com.linbit.utils.Base64;

public class SignInEntryPojo
{
    private final String identityName;
    private final String roleName;
    private final String domainName;
    private final Long rolePrivileges;
    private final String saltBase64;
    private final String hashBase64;

    public SignInEntryPojo(
        final String identityNameRef,
        final String roleNameRef,
        final String domainNameRef,
        final Long rolePrivilegesRef,
        final String saltBase64Ref,
        final String hashBase64Ref
    )
    {
        identityName = identityNameRef;
        roleName = roleNameRef;
        domainName = domainNameRef;
        rolePrivileges = rolePrivilegesRef;
        saltBase64 = saltBase64Ref;
        hashBase64 = hashBase64Ref;
    }

    public String getIdentityName()
    {
        return identityName;
    }

    public String getRoleName()
    {
        return roleName;
    }

    public String getDomainName()
    {
        return domainName;
    }

    public long getRolePrivileges()
    {
        return rolePrivileges;
    }

    public byte[] getSalt() throws IllegalArgumentException
    {
        return saltBase64 != null ? Base64.decode(saltBase64.trim()) : null;
    }

    public String getSaltBase64()
    {
        return saltBase64;
    }

    public String getHashBase64()
    {
        return hashBase64;
    }

    public byte[] getHash() throws IllegalArgumentException
    {
        return hashBase64 != null ? Base64.decode(hashBase64.trim()) : null;
    }
}
