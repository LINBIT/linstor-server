package com.linbit.linstor.core;

import com.linbit.linstor.annotation.Nullable;

import javax.inject.Inject;

import com.google.inject.Singleton;

@Singleton
public class StltSecurityObjects
{
    private @Nullable byte[] cryptKey;
    private @Nullable byte[] hash;
    private @Nullable byte[] salt;
    private @Nullable byte[] encKey;

    @Inject
    public StltSecurityObjects()
    {
    }

    public void setCryptKey(byte[] cryptKeyRef, byte[] hashRef, byte[] saltRef, byte[] encKeyRef)
    {
        cryptKey = cryptKeyRef;
        hash = hashRef;
        salt = saltRef;
        encKey = encKeyRef;
    }

    public @Nullable byte[] getCryptKey()
    {
        return cryptKey;
    }

    public @Nullable byte[] getHash()
    {
        return hash;
    }

    public @Nullable byte[] getSalt()
    {
        return salt;
    }

    public @Nullable byte[] getEncKey()
    {
        return encKey;
    }
}
