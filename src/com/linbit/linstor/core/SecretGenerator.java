package com.linbit.linstor.core;

import com.linbit.utils.Base64;

import java.security.SecureRandom;

public class SecretGenerator
{
    // Random data size for automatic DRBD shared secret generation
    // The random data will be Base64 encoded, so the length of the
    // shared secret string will be (SECRET_LEN + 2) / 3 * 4
    public static final int DRBD_SHARED_SECRET_SIZE = 15;

    /**
     * Generates a random value for a DRBD resource's shared secret
     *
     * @return a 20 character long random String
     */
    static String generateSharedSecret()
    {
        return generateSecretString(DRBD_SHARED_SECRET_SIZE);
    }

    /**
     * Generates a random String.<br />
     * <br />
     * NOTE: the size is likely to differ from the resulting String's length.
     * The size parameter specifies the count of bytes generated which then are
     * Base64-encoded. That encoding will very likely result in a longer String.
     *
     * @param size
     * @return A Base64 encoded String of <code>size</code> random bytes.
     */
    static String generateSecretString(int size)
    {
        String secret = Base64.encode(generateSecret(size));
        return secret;
    }

    /**
     * @param size
     * @return An array of random bytes with the length of the <code>size</code> parameter
     */
    static byte[] generateSecret(int size)
    {
        byte[] randomBytes = new byte[size];
        new SecureRandom().nextBytes(randomBytes);
        return randomBytes;
    }

    private SecretGenerator()
    {
    }
}
