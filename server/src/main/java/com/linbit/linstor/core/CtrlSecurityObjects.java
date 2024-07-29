package com.linbit.linstor.core;

import com.linbit.linstor.annotation.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlSecurityObjects
{

    /**
     * Key used as a passphrase to encrypt persisted volume definition encryption keys.
     * Satellites will also need this masterKey to decrypt the key they need for dm-crypt.
     */
    private @Nullable byte[] cryptKey;
    private @Nullable byte[] cryptHash;
    private @Nullable byte[] cryptSalt;
    private @Nullable byte[] encKey;

    @Inject
    public CtrlSecurityObjects()
    {
    }

    public void setCryptKey(
        @Nullable byte[] cryptKeyRef,
        @Nullable byte[] cryptHashRef,
        @Nullable byte[] cryptSaltRef,
        @Nullable byte[] encKeyRef
    )
    {
        cryptKey = cryptKeyRef;
        cryptHash = cryptHashRef;
        cryptSalt = cryptSaltRef;
        encKey = encKeyRef;
    }

    public @Nullable byte[] getCryptKey()
    {
        return cryptKey;
    }

    public @Nullable byte[] getCryptHash()
    {
        return cryptHash;
    }

    public @Nullable byte[] getCryptSalt()
    {
        return cryptSalt;
    }

    public @Nullable byte[] getEncKey()
    {
        return encKey;
    }

    public boolean areAllSet()
    {
        return cryptKey != null && cryptHash != null && cryptSalt != null && encKey != null;
    }
}
