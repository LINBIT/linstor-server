package com.linbit.crypto;

import com.linbit.linstor.modularcrypto.CryptoProviderLoader;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@PowerMockIgnore({"com.sun.*", "javax.*"})
@RunWith(PowerMockRunner.class)
public class SymmetricKeyCipherTest
{
    private static byte[] cipherKey = "1234567890123456".getBytes();

    private ModularCryptoProvider cryptoProvider;
    private ByteArrayCipher cipher;

    @Before
    public void setUp() throws Exception
    {
        cryptoProvider = CryptoProviderLoader.loadCryptoProvider();
        cipher = cryptoProvider.createCipherWithKey(cipherKey);
    }

    @Test
    public void simpleEncryptAndDecrypt() throws Exception
    {
        byte[] plainText = "1234567890".getBytes();
        byte[] encrypted = cipher.encrypt(plainText);
        byte[] decrypted = cipher.decrypt(encrypted);

        assertArrayEquals(plainText, decrypted);
    }

    @Test
    public void longInputEncryptAndDecrypt() throws Exception
    {
        final byte[] binaryKey = new byte[]
        {
            'k', 'E', 'o', '$', 'C', 'k', '3', '0', '&', '&', 'd', 'k', 'M', '!', 'k', '2'
        };
        String inputProp = "Linstor property encryption test with longer input";
        try
        {
            ByteArrayCipher encrypter = cryptoProvider.createCipherWithKey(binaryKey);
            byte[] encryptedProp = encrypter.encrypt(inputProp.getBytes());
            ByteArrayCipher decrypter = cryptoProvider.createCipherWithKey(binaryKey);
            String decryptedProp = new String(decrypter.decrypt(encryptedProp));

            assertEquals(inputProp, decryptedProp);
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.err);
        }
    }
}
