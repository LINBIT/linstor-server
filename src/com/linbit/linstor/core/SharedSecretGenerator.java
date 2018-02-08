package com.linbit.linstor.core;

import com.linbit.utils.Base64;

import java.security.SecureRandom;

public class SharedSecretGenerator
{
    // Random data size for automatic DRBD shared secret generation
    // The random data will be Base64 encoded, so the length of the
    // shared secret string will be (SECRET_LEN + 2) / 3 * 4
    private static final int DRBD_SHARED_SECRET_SIZE = 15;

    /**
     * Generates a random value for a DRBD resource's shared secret
     *
     * @return a 20 character long random String
     */
    static String generateSharedSecret()
    {
        byte[] randomBytes = new byte[DRBD_SHARED_SECRET_SIZE];
        new SecureRandom().nextBytes(randomBytes);
        String secret = Base64.encode(randomBytes);
        return secret;
    }

    private SharedSecretGenerator()
    {
    }
}
