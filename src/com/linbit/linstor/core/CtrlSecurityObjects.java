package com.linbit.linstor.core;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlSecurityObjects
{

    /**
     * Key used as a passphrase to encrypt persisted volume definition encryption keys.
     * Satellites will also need this masterKey to decrypt the key they need for dm-crypt.
     */
    private byte[] cryptKey;

    @Inject
    public CtrlSecurityObjects()
    {
    }

    public byte[] getCryptKey()
    {
        return cryptKey;
    }

    public void setCryptKey(byte[] cryptKeyRef)
    {
        cryptKey = cryptKeyRef;
    }
}
