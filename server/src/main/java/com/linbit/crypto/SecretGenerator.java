package com.linbit.crypto;

public interface SecretGenerator
{
    /**
     * Generates a random value for a DRBD resource's shared secret
     */
    String generateDrbdSharedSecret();

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
    String generateSecretString(int size);

    /**
     * Generates a byte array of the specified size, filled with random data
     *
     * @param size The size of the byte array to generate
     * @return An array of random bytes with the length of the <code>size</code> parameter
     */
    byte[] generateSecret(int size);
}
