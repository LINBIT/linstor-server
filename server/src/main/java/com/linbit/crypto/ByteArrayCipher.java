package com.linbit.crypto;

import com.linbit.linstor.LinStorException;

/**
 * Interface for ciphers that allow simple encryption and decryption of byte arrays
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ByteArrayCipher
{
    /**
     * Encrypts {@code plainText} and returns a data structure containing the resulting cipher text
     *
     * The data returned by this method is suitable as input for the {@code decrypt()} method.<br/>
     * For this reason, the returned data structure may contain further information in addition
     * to the cipher text, depending on the cipher's requirements for decryption by the {@code decrypt()}
     * method.
     *
     * @param plainText The data to encrypt
     * @return byte array containing the result of the encryption
     */
    byte[] encrypt(byte[] plainText) throws LinStorException;

    /**
     * Decrypts {@code cipherText} and returns the resulting plain text as a new byte array
     *
     * {@code cipherText} is expected to contain all required information for decryption. This may include
     * further information in addition to the cipher text of the original plain text, depending on the
     * requirements of the cipher implementation being used.<br/>
     * Data returned by the {@code encrypt()} method is suitable as input for this method.
     *
     * @param cipherText The data to decrypt
     * @return byte array containing the result of the decryption
     */
    byte[] decrypt(byte[] cipherText) throws LinStorException;
}
