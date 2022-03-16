package com.linbit.linstor.modularcrypto;

import com.linbit.crypto.SecretGenerator;
import com.linbit.utils.Base64;
import java.security.SecureRandom;

public class SecretGeneratorImpl implements SecretGenerator
{
    // Random data size for automatic DRBD shared secret generation
    // The random data will be Base64 encoded, so the length of the
    // shared secret string will be (DRBD_SHARED_SECRET_SIZE + 2) / 3 * 4
    public static final int DRBD_SHARED_SECRET_SIZE = 15;

    private final SecureRandom rnd;

    SecretGeneratorImpl(final SecureRandom rndRef)
    {
        rnd = rndRef;
    }

    /**
     * Generates a random value for a DRBD resource's shared secret
     *
     * @return a 20 character long random String
     */
    @Override
    public String generateDrbdSharedSecret()
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
     * @param size The number of bytes of random data to translate into a String (not the length of the String)
     * @return A Base64 encoded String of <code>size</code> random bytes.
     */
    @Override
    public String generateSecretString(final int size)
    {
        final byte[] randomBytes = generateSecret(size);
        final String secret = Base64.encode(randomBytes);
        return secret;
    }

    /**
     * Generates a byte array of the specified size, filled with random data
     *
     * @param size The size of the byte array to generate
     * @return An array of random bytes with the length of the <code>size</code> parameter
     */
    @Override
    public byte[] generateSecret(final int size)
    {
        byte[] randomBytes = new byte[size];
        rnd.nextBytes(randomBytes);
        return randomBytes;
    }
}
