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
    private byte[] cryptHash;
    private byte[] cryptSalt;
    private byte[] encKey;

    @Inject
    public CtrlSecurityObjects()
    {
    }

    public void setCryptKey(byte[] cryptKeyRef, byte[] cryptHashRef, byte[] cryptSaltRef, byte[] encKeyRef)
    {
        cryptKey = cryptKeyRef;
        cryptHash = cryptHashRef;
        cryptSalt = cryptSaltRef;
        encKey = encKeyRef;
    }

    public byte[] getCryptKey()
    {
        return cryptKey;
    }

    public byte[] getCryptHash()
    {
        return cryptHash;
    }

    public byte[] getCryptSalt()
    {
        return cryptSalt;
    }

    public byte[] getEncKey()
    {
        return encKey;
    }

    public boolean areAllSet()
    {
        return cryptKey != null && cryptHash != null && cryptSalt != null && encKey != null;
    }
}
