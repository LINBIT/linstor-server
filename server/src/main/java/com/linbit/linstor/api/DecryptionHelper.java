package com.linbit.linstor.api;

import com.linbit.crypto.LengthPadding;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.linstor.LinStorException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DecryptionHelper
{
    private final LengthPadding cryptoLenPad;

    @Inject
    public DecryptionHelper(
        LengthPadding cryptoLenPadRef
    )
    {
        cryptoLenPad = cryptoLenPadRef;
    }

    public byte[] decrypt(byte[] masterKey, byte[] encryptedKey) throws LinStorException
    {
        SymmetricKeyCipher cipher = SymmetricKeyCipher.getInstanceWithKey(masterKey);
        byte[] decryptedData = cipher.decrypt(encryptedKey);
        byte[] decryptedKey = cryptoLenPad.retrieve(decryptedData);

        return decryptedKey;
    }
}
