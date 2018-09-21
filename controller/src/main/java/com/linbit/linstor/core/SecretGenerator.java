package com.linbit.linstor.core;

import com.linbit.utils.Base64;

import java.security.SecureRandom;

public class SecretGenerator
{
    // SecureRandom is multithreading-safe
    public static final SecureRandom RND = new SecureRandom();

    // Random data size for automatic DRBD shared secret generation
    // The random data will be Base64 encoded, so the length of the
    // shared secret string will be (SECRET_LEN + 2) / 3 * 4
    public static final int DRBD_SHARED_SECRET_SIZE = 15;

    /**
     * Generates a random value for a DRBD resource's shared secret
     *
     * @return a 20 character long random String
     */
    public static String generateSharedSecret()
    {
        return generateSecretString(DRBD_SHARED_SECRET_SIZE);
    }

    /**
     * Generates a random String.<br />
     * <br />
     * NOTE: The {@code size} parameter specifies the number of random bytes to
     *       generate, but the length of the String that is returned by this
     *       method will probably not match the specified {@code size} due
     *       to side effects of encoding a byte array into a String that is
     *       composed of unicode characters.
     *
     * @param size
     * @return A Base64 encoded String of <code>size</code> random bytes.
     */
    public static String generateSecretString(int size)
    {
        String secret = Base64.encode(generateSecret(size));
        return secret;
    }

    /**
     * @param size
     * @return An array of random bytes with the length of the <code>size</code> parameter
     */
    public static byte[] generateSecret(int size)
    {
        byte[] randomBytes = new byte[size];
        RND.nextBytes(randomBytes);
        return randomBytes;
    }

    private SecretGenerator()
    {
    }
}
