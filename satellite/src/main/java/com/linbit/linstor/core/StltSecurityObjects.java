package com.linbit.linstor.core;

import javax.inject.Inject;

import com.google.inject.Singleton;

@Singleton
public class StltSecurityObjects
{
    private byte[] cryptKey;
    private byte[] hash;
    private byte[] salt;
    private byte[] encKey;

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

    public byte[] getCryptKey()
    {
        return cryptKey;
    }

    public byte[] getHash()
    {
        return hash;
    }

    public byte[] getSalt()
    {
        return salt;
    }

    public byte[] getEncKey()
    {
        return encKey;
    }
}
