package com.linbit.linstor.api;

import com.linbit.crypto.ByteArrayCipher;
import com.linbit.crypto.LengthPadding;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DecryptionHelper
{
    private final LengthPadding cryptoLenPad;
    private final ModularCryptoProvider cryptoProvider;

    @Inject
    public DecryptionHelper(
        LengthPadding cryptoLenPadRef,
        ModularCryptoProvider cryptoProviderRef
    )
    {
        cryptoLenPad = cryptoLenPadRef;
        cryptoProvider = cryptoProviderRef;
    }

    public byte[] decrypt(byte[] masterKey, byte[] encryptedKey) throws LinStorException
    {
        ByteArrayCipher cipher = cryptoProvider.createCipherWithKey(masterKey);
        byte[] decryptedData = cipher.decrypt(encryptedKey);
        byte[] decryptedKey = cryptoLenPad.retrieve(decryptedData);

        return decryptedKey;
    }
}
