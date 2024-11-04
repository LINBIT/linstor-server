package com.linbit.linstor.modularcrypto;

import com.linbit.crypto.ByteArrayCipher;
import com.linbit.linstor.LinStorException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Cipher utility for simple encryption and decryption of byte arrays
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class JclSymmetricKeyCipher implements ByteArrayCipher
{
    // Specification of the cryptographic algorithm, encryption/decryption mode and block padding
    public static final String CIPHER_SPEC  = "AES";
    public static final String MODE_SPEC    = "CFB";
    public static final String PAD_SPEC     = "NoPadding";
    private static final String CIPHER_ALGORITHM = CIPHER_SPEC + "/" + MODE_SPEC + "/" + PAD_SPEC;
    // Length of the initialization vector for the selected algorithm
    public static final int    IV_LENGTH    = 16;

    // Key derivation algorithm for key generation from a password
    public static final String KD_ALG_SPEC  = "PBKDF2WithHmacSHA1";

    // Number of iterations for key derivation from a password
    public static final int KD_ITERATIONS = 5000;

    private static final int BITS_PER_BYTE = 8;

    // spotbugs does not realize that this field gets initialized in the static block due to a bug
    @SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    private static CipherStrength defaultCipherStrength;

    static
    {
        defaultCipherStrength = CipherStrength.KEY_LENGTH_128;
    }

    private final Cipher crypto;
    private final SecretKey encryptionKey;
    private final SecureRandom rnd;

    public static CipherStrength getDefaultCipherStrength()
    {
        return defaultCipherStrength;
    }

    public static void setDefaultCipherStrength(final CipherStrength keyLength)
    {
        defaultCipherStrength = keyLength;
    }

    /**
     * Returns a new instance of the cipher utility using {@code key} as the encryption key
     *
     * The length of {@code key} must be suitable for use with the cipher algorithm.
     *
     * @param key the encryption key to use with the cipher algorithm
     * @return new instance of the cipher utility
     * @throws LinStorException if the creation of a new instance of the cipher utility fails
     */
    public static JclSymmetricKeyCipher getInstanceWithKey(
        final byte[] key
    )
        throws LinStorException
    {
        int keyBits = key.length * BITS_PER_BYTE;
        if (keyBits != CipherStrength.KEY_LENGTH_128.keyLength &&
            keyBits != CipherStrength.KEY_LENGTH_192.keyLength &&
            keyBits != CipherStrength.KEY_LENGTH_256.keyLength)
        {
            throw new LinStorException(
                "Unsupported key length of " + keyBits + " bits for algorithm " + CIPHER_ALGORITHM,
                "The initialization of a cryptographic cipher failed",
                "Initialization of the cipher was attempted with an unsupported key length",
                "This problem is typically caused by an incompatible combination of encryption parameters.\n" +
                "Please contact product support for analysis and correction of this problem.",
                "The length of the key that caused the problem was " + keyBits + " bits",
                null
            );
        }
        SecretKeySpec cipherKey = new SecretKeySpec(key, CIPHER_SPEC);
        return new JclSymmetricKeyCipher(cipherKey);
    }

    /**
     * Returns a new instance of the cipher utility using a key derived from the specified salted password as
     * the encryption key
     *
     * The encryption key is derived from the password by applying a password based key derivation function.
     * The strength of the resulting key is specified by the currently selected default cipher strength.
     *
     * @param salt the salt value to combine with the password
     * @param password the password to generate the encryption key from; this will be cleared by filling with zeros
     * @return new instance of the cipher utility
     * @throws LinStorException if the creation of a new instance of the cipher utility fails
     */
    public static JclSymmetricKeyCipher getInstanceWithPassword(
        final byte[] password,
        final byte[] salt
    )
        throws LinStorException
    {
        return getInstanceWithPassword(password, salt, defaultCipherStrength);
    }

    /**
     * Returns a new instance of the cipher utility using a key derived from the specified salted password as
     * the encryption key
     *
     * The encryption key is derived from the password by applying a password based key derivation function.
     * The strength of the resulting key is specified by the {@code csSpec} parameter.
     *
     * @param salt the salt value to combine with the password
     * @param password the password to generate the encryption key from; this will be cleared by filling with zeros
     * @param csSpec the strength of the encryption key that will be derived from the (salted) password
     * @return new instance of the cipher utility
     * @throws LinStorException if the creation of a new instance of the cipher utility fails
     */
    public static JclSymmetricKeyCipher getInstanceWithPassword(
        final byte[] password,
        final byte[] salt,
        final CipherStrength csSpec
    )
        throws LinStorException
    {
        JclSymmetricKeyCipher instance;

        // Key initialization expects a character array, however, many algorithms will
        // only use the low-order 8 bits of each character. For this reason, the password
        // is only accepted as a byte array, and is then converted to a character array internally.
        final char[] passwordChars = new char[password.length];
        for (int idx = 0; idx < passwordChars.length; ++idx)
        {
            passwordChars[idx] = (char) password[idx];
        }

        PBEKeySpec keySpec = new PBEKeySpec(passwordChars, salt, KD_ITERATIONS, csSpec.keyLength);
        try
        {
            SecretKey derivedKey = SecretKeyFactory.getInstance(KD_ALG_SPEC).generateSecret(keySpec);
            SecretKeySpec cipherKey = new SecretKeySpec(derivedKey.getEncoded(), CIPHER_SPEC);
            instance = new JclSymmetricKeyCipher(cipherKey);
        }
        catch (NoSuchAlgorithmException algExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: The key derivation algorithm '" + KD_ALG_SPEC + "' is not supported",
                "The initialization of a cryptographic cipher failed",
                "The requested key derivation algorithm '" + KD_ALG_SPEC + "' is not available on this system",
                "Installation of additional components may be necessary to add support " +
                "for the encryption algorithm.",
                null,
                algExc
            );
        }
        catch (InvalidKeySpecException keySpecExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: The key specification generated from the supplied password " +
                "is not valid",
                "The initialization of a cryptographic cipher failed",
                "The key specification generated from the supplied password is not valid",
                "- The password may not be suitable. Try using a different password.\n" +
                "  Make sure the password has reasonable length and content.\n" +
                "- Try changing the cryptographic settings, such as the key length for\n" +
                "  the encryption algorithm.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                keySpecExc
            );
        }
        finally
        {
            Arrays.fill(passwordChars, (char) 0);
        }
        return instance;
    }


    private JclSymmetricKeyCipher(final SecretKey key)
        throws LinStorException
    {
        rnd = new SecureRandom();
        encryptionKey = key;
        try
        {
            crypto = Cipher.getInstance(CIPHER_ALGORITHM);
        }
        catch (NoSuchAlgorithmException algExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: The requested cipher '" + CIPHER_ALGORITHM + "' is not supported",
                "The initialization of a cryptographic cipher failed",
                "The requested cipher algorithm '" + CIPHER_SPEC + "' is not available on this system",
                "Installation of additional components may be necessary to add support " +
                "for the encryption algorithm.\n" +
                "Support for some cryptographic algorithms with strong encryption keys requires the\n" +
                "installation of the JCE Unlimited Strength Jurisdiction Policy Files.",
                null,
                algExc
            );
        }
        catch (NoSuchPaddingException padExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: The padding scheme of the requestes cipher '" + CIPHER_ALGORITHM +
                "' is not supported",
                "The initialization of a cryptographic cipher failed",
                "The padding scheme '" + PAD_SPEC + "' is not supported",
                "Installation of additional components may be necessary to add support " +
                "for the requested padding scheme.",
                null,
                padExc
            );
        }
    }

    @Override
    public byte[] encrypt(final byte[] plainText)
        throws LinStorException
    {
        final byte[] cipherText;
        try
        {
            // Generate a random IV
            byte[] iv = new byte[IV_LENGTH];
            rnd.nextBytes(iv);
            IvParameterSpec ivPrm = new IvParameterSpec(iv);

            // Initialize the cipher with the encryption key and the IV and encrypt the data
            crypto.init(Cipher.ENCRYPT_MODE, encryptionKey, ivPrm);
            byte[] cryptoOut = crypto.doFinal(plainText);

            // Chain the IV and the encrypted data
            cipherText = new byte[iv.length + cryptoOut.length];
            System.arraycopy(iv, 0, cipherText, 0, iv.length);
            System.arraycopy(cryptoOut, 0, cipherText, iv.length, cipherText.length - iv.length);
        }
        catch (InvalidKeyException keyExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid encryption key for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The encryption key is not valid for use with the selected encryption algorithm",
                "Installation of additional components may be necessary to add support " +
                "for using the encryption algorithm with stronger encryption keys.\n" +
                "Support for some cryptographic algorithms with strong encryption keys requires the\n" +
                "installation of the JCE Unlimited Strength Jurisdiction Policy Files.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                keyExc
            );
        }
        catch (InvalidAlgorithmParameterException prmExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid initialization vector for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The initialization vector is not valid for use with the selected encryption algorithm",
                "This problem is typically caused by an incompatible combination of encryption parameters.\n" +
                "Please contact product support for analysis and correction of this problem.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                prmExc
            );
        }
        catch (IllegalBlockSizeException blkSizeExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid block size for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The selected encryption algorithm does not support the block size used\n" +
                "by the cryptographic subsystem",
                "This problem is typically caused by an incompatible combination of encryption parameters.\n" +
                "Please contact product support for analysis and correction of this problem.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                blkSizeExc
            );
        }
        catch (BadPaddingException padExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid block padding for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The selected encryption algorithm does not support the selected padding mode",
                "This problem is typically caused by an incompatible combination of encryption parameters.\n" +
                "Please contact product support for analysis and correction of this problem.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                padExc
            );
        }
        return cipherText;
    }

    @Override
    public byte[] decrypt(final byte[] cipherText)
        throws LinStorException
    {
        if (cipherText.length < IV_LENGTH)
        {
            throw new IllegalArgumentException();
        }

        final byte[] plainText;
        try
        {
            // Extract the IV from the data structure
            byte[] iv = new byte[IV_LENGTH];
            System.arraycopy(cipherText, 0, iv, 0, iv.length);
            IvParameterSpec ivPrm = new IvParameterSpec(iv);

            // Extract the encrypted data from the data structure
            byte[] cryptoIn = new byte[cipherText.length - iv.length];
            System.arraycopy(cipherText, iv.length, cryptoIn, 0, cryptoIn.length);

            // Initialize the cipher with the encryption key and the IV and decrypt the data
            crypto.init(Cipher.DECRYPT_MODE, encryptionKey, ivPrm);
            plainText = crypto.doFinal(cryptoIn);
        }
        catch (InvalidKeyException keyExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid encryption key for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The encryption key is not valid for use with the selected encryption algorithm",
                "Installation of additional components may be necessary to add support " +
                "for using the encryption algorithm with stronger encryption keys.\n" +
                "Support for some cryptographic algorithms with strong encryption keys requires the\n" +
                "installation of the JCE Unlimited Strength Jurisdiction Policy Files.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                keyExc
            );
        }
        catch (InvalidAlgorithmParameterException prmExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid initialization vector for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The initialization vector is not valid for use with the selected encryption algorithm",
                "This problem is typically caused by an incompatible combination of encryption parameters.\n" +
                "Please contact product support for analysis and correction of this problem.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                prmExc
            );
        }
        catch (IllegalBlockSizeException blkSizeExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid block size for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The selected encryption algorithm does not support the block size used\n" +
                "by the cryptographic subsystem",
                "This problem is typically caused by an incompatible combination of encryption parameters.\n" +
                "Please contact product support for analysis and correction of this problem.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                blkSizeExc
            );
        }
        catch (BadPaddingException padExc)
        {
            throw new LinStorException(
                "Cipher initialization failed: Invalid block padding for algorithm '" + CIPHER_ALGORITHM + "'",
                "The initialization of a cryptographic cipher failed",
                "The selected encryption algorithm does not support the selected padding mode",
                "This problem is typically caused by an incompatible combination of encryption parameters.\n" +
                "Please contact product support for analysis and correction of this problem.",
                "The currently selected algorithm is " + CIPHER_SPEC + " in " + MODE_SPEC + " mode with the " +
                PAD_SPEC + " padding scheme",
                padExc
            );
        }
        return plainText;
    }
}
