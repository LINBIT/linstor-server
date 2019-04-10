package com.linbit.crypto;

import java.security.SecureRandom;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@PowerMockIgnore({"com.sun.*", "javax.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(SymmetricKeyCipher.class)
public class SymmetricKeyCiperTest
{
    private static byte[] cipherKey = "1234567890123456".getBytes();
    private SymmetricKeyCipher cipher;

    private NotSoRandom random = new NotSoRandom();
    private byte[] iv = new byte[SymmetricKeyCipher.IV_LENGTH];

    @Before
    public void setUp() throws Exception
    {
        // PowerMockito.whenNew(SecureRandom.class).withAnyArguments().thenReturn(random);
        // Arrays.fill(iv, (byte) 0);

        cipher = SymmetricKeyCipher.getInstanceWithKey(cipherKey);
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
            ByteArrayCipher encrypter = SymmetricKeyCipher.getInstanceWithKey(binaryKey);
            byte[] encryptedProp = encrypter.encrypt(inputProp.getBytes());
            ByteArrayCipher decrypter = SymmetricKeyCipher.getInstanceWithKey(binaryKey);
            String decryptedProp = new String(decrypter.decrypt(encryptedProp));

            assertEquals(inputProp, decryptedProp);
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.err);
        }
    }

    private class NotSoRandom extends SecureRandom
    {
        private static final long serialVersionUID = -4562395783864498971L;

        @Override
        public void nextBytes(byte[] bytes)
        {
            System.arraycopy(iv, 0, bytes, 0, iv.length);
        }
    }
}
