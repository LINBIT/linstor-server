package com.linbit.linstor.security.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.utils.Base64;

public class SignInEntryPojo
{
    private final @Nullable String identityName;
    private final @Nullable String roleName;
    private final @Nullable String domainName;
    private final @Nullable Long rolePrivileges;
    private final @Nullable String saltBase64;
    private final @Nullable String hashBase64;

    public SignInEntryPojo(
        final @Nullable String identityNameRef,
        final @Nullable String roleNameRef,
        final @Nullable String domainNameRef,
        final @Nullable Long rolePrivilegesRef,
        final @Nullable String saltBase64Ref,
        final @Nullable String hashBase64Ref
    )
    {
        identityName = identityNameRef;
        roleName = roleNameRef;
        domainName = domainNameRef;
        rolePrivileges = rolePrivilegesRef;
        saltBase64 = saltBase64Ref;
        hashBase64 = hashBase64Ref;
    }

    public @Nullable String getIdentityName()
    {
        return identityName;
    }

    public @Nullable String getRoleName()
    {
        return roleName;
    }

    public @Nullable String getDomainName()
    {
        return domainName;
    }

    public @Nullable Long getRolePrivileges()
    {
        return rolePrivileges;
    }

    public byte[] getSalt() throws IllegalArgumentException
    {
        return saltBase64 != null ? Base64.decode(saltBase64.trim()) : null;
    }

    public @Nullable String getSaltBase64()
    {
        return saltBase64;
    }

    public @Nullable String getHashBase64()
    {
        return hashBase64;
    }

    public byte[] getHash() throws IllegalArgumentException
    {
        return hashBase64 != null ? Base64.decode(hashBase64.trim()) : null;
    }
}
