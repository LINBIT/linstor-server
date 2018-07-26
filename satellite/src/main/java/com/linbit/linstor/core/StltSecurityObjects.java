package com.linbit.linstor.core;

import javax.inject.Inject;

import com.google.inject.Singleton;

@Singleton
public class StltSecurityObjects
{
    private byte[] cryptKey;

    @Inject
    public StltSecurityObjects()
    {
    }

    public void setCryptKey(byte[] cryptKeyRef)
    {
        cryptKey = cryptKeyRef;
    }

    public byte[] getCryptKey()
    {
        return cryptKey;
    }
}
