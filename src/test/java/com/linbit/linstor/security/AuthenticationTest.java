package com.linbit.linstor.security;

import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.modularcrypto.CryptoProviderLoader;
import com.linbit.crypto.KeyDerivation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

// TODO: add more tests
public class AuthenticationTest
{
    public AuthenticationTest()
    {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testPasswordsMatch() throws Exception
    {
        final ModularCryptoProvider cryptoProvider = CryptoProviderLoader.loadCryptoProvider();
        final KeyDerivation keyDrv = cryptoProvider.createKeyDerivation();

        {
            byte[] password = "AllW0rk&NoPlayMakesJ0nnyADullB0y".getBytes();

            byte[] salt =
            {
                (byte) 0x9D, (byte) 0x61, (byte) 0x9D, (byte) 0x37,
                (byte) 0xC6, (byte) 0x46, (byte) 0x1A, (byte) 0x94,
                (byte) 0xB0, (byte) 0x44, (byte) 0xB4, (byte) 0xD1,
                (byte) 0xF3, (byte) 0x8D, (byte) 0x80, (byte) 0x15
            };

            byte[] hash =
            {
                (byte) 0xF9, (byte) 0x07, (byte) 0x77, (byte) 0x5C,
                (byte) 0x7E, (byte) 0xA2, (byte) 0x59, (byte) 0x40,
                (byte) 0x83, (byte) 0x72, (byte) 0x9F, (byte) 0x85,
                (byte) 0x2D, (byte) 0x82, (byte) 0x35, (byte) 0xB4,
                (byte) 0x5B, (byte) 0xDF, (byte) 0x3F, (byte) 0x35,
                (byte) 0xD8, (byte) 0x3B, (byte) 0x0F, (byte) 0x69,
                (byte) 0xB6, (byte) 0x40, (byte) 0x51, (byte) 0xA8,
                (byte) 0x11, (byte) 0xF6, (byte) 0xBE, (byte) 0xA2,
                (byte) 0x99, (byte) 0x74, (byte) 0xCD, (byte) 0x13,
                (byte) 0xF0, (byte) 0x2C, (byte) 0x3C, (byte) 0xD0,
                (byte) 0xEE, (byte) 0xDF, (byte) 0x0A, (byte) 0x75,
                (byte) 0xC1, (byte) 0x0F, (byte) 0x07, (byte) 0xBF,
                (byte) 0xB8, (byte) 0x92, (byte) 0x60, (byte) 0x84,
                (byte) 0x61, (byte) 0x60, (byte) 0x2D, (byte) 0xFD,
                (byte) 0x9F, (byte) 0x12, (byte) 0xCE, (byte) 0x03,
                (byte) 0x00, (byte) 0xEB, (byte) 0x1E, (byte) 0xA4
            };

            assertTrue(
                "Password check failed for a correct password",
                Authentication.passwordMatches(keyDrv, password, salt, hash)
            );
        }

        {
            byte[] password = "NoPlay&AllW0rkMakesJ0nnyAD0llBuoy".getBytes();

            byte[] salt =
            {
                (byte) 0x9D, (byte) 0x61, (byte) 0x9D, (byte) 0x37,
                (byte) 0xC6, (byte) 0x46, (byte) 0x1A, (byte) 0x94,
                (byte) 0xB0, (byte) 0x44, (byte) 0xB4, (byte) 0xD1,
                (byte) 0xF3, (byte) 0x8D, (byte) 0x80, (byte) 0x15
            };

            byte[] hash =
            {
                (byte) 0xF9, (byte) 0x07, (byte) 0x77, (byte) 0x5C,
                (byte) 0x7E, (byte) 0xA2, (byte) 0x59, (byte) 0x40,
                (byte) 0x83, (byte) 0x72, (byte) 0x9F, (byte) 0x85,
                (byte) 0x2D, (byte) 0x82, (byte) 0x35, (byte) 0xB4,
                (byte) 0x5B, (byte) 0xDF, (byte) 0x3F, (byte) 0x35,
                (byte) 0xD8, (byte) 0x3B, (byte) 0x0F, (byte) 0x69,
                (byte) 0xB6, (byte) 0x40, (byte) 0x51, (byte) 0xA8,
                (byte) 0x11, (byte) 0xF6, (byte) 0xBE, (byte) 0xA2,
                (byte) 0x99, (byte) 0x74, (byte) 0xCD, (byte) 0x13,
                (byte) 0xF0, (byte) 0x2C, (byte) 0x3C, (byte) 0xD0,
                (byte) 0xEE, (byte) 0xDF, (byte) 0x0A, (byte) 0x75,
                (byte) 0xC1, (byte) 0x0F, (byte) 0x07, (byte) 0xBF,
                (byte) 0xB8, (byte) 0x92, (byte) 0x60, (byte) 0x84,
                (byte) 0x61, (byte) 0x60, (byte) 0x2D, (byte) 0xFD,
                (byte) 0x9F, (byte) 0x12, (byte) 0xCE, (byte) 0x03,
                (byte) 0x00, (byte) 0xEB, (byte) 0x1E, (byte) 0xA4
            };

            assertFalse(
                "Password check succeeded for an incorrect password",
                Authentication.passwordMatches(keyDrv, password, salt, hash)
            );
        }
    }
}
