package com.linbit.linstor.api;

import com.linbit.crypto.ByteArrayCipher;
import com.linbit.crypto.LengthPadding;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DecryptionHelper
{
    private final LengthPadding cryptoLenPad;
    private final ModularCryptoProvider cryptoProvider;

    @Inject
    public DecryptionHelper(
        ModularCryptoProvider cryptoProviderRef
    )
    {
        cryptoProvider = cryptoProviderRef;
        cryptoLenPad = cryptoProviderRef.createLengthPadding();
    }

    public byte[] decrypt(byte[] masterKey, byte[] encryptedKey) throws LinStorException
    {
        ByteArrayCipher cipher = cryptoProvider.createCipherWithKey(masterKey);
        byte[] decryptedData = cipher.decrypt(encryptedKey);
        byte[] decryptedKey = cryptoLenPad.retrieve(decryptedData);

        return decryptedKey;
    }

    public String decryptB64ToString(byte[] masterKey, String encryptedKeyB64) throws LinStorException
    {
        byte[] encryptedKey = Base64.decode(encryptedKeyB64);
        return new String(decrypt(masterKey, encryptedKey));
    }
}
