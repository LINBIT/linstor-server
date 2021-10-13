package com.linbit.linstor.security;

import java.security.MessageDigest;
import javax.crypto.SecretKeyFactory;
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
        final SecretKeyFactory keyFact = SecretKeyFactory.getInstance("PBKDF2WITHHMACSHA512");
        {
            byte[] password = "AllW0rk&NoPlayMakesJ0nnyADullB0y".getBytes();
            byte[] salt =
            {
                (byte) 0x21, (byte) 0x39, (byte) 0xC9, (byte) 0x75,
                (byte) 0x9B, (byte) 0xBB, (byte) 0x93, (byte) 0x5F,
                (byte) 0x9D, (byte) 0xAB, (byte) 0x0D, (byte) 0x08,
                (byte) 0x7A, (byte) 0x4F, (byte) 0xA3, (byte) 0x9F
            };

            byte[] hash =
            {
                (byte) 0xD3, (byte) 0xFA, (byte) 0xF8, (byte) 0x82,
                (byte) 0x7E, (byte) 0x5D, (byte) 0x6D, (byte) 0xD1,
                (byte) 0xEB, (byte) 0x6C, (byte) 0xE5, (byte) 0xF7,
                (byte) 0x6E, (byte) 0x81, (byte) 0xC5, (byte) 0xF9,
                (byte) 0x25, (byte) 0xDF, (byte) 0x71, (byte) 0x07,
                (byte) 0x88, (byte) 0x19, (byte) 0x31, (byte) 0x52,
                (byte) 0xA0, (byte) 0xCF, (byte) 0x28, (byte) 0x8F,
                (byte) 0xB4, (byte) 0x68, (byte) 0x6F, (byte) 0x44,
                (byte) 0x70, (byte) 0x4E, (byte) 0x19, (byte) 0xAE,
                (byte) 0x3C, (byte) 0xF5, (byte) 0x60, (byte) 0x81,
                (byte) 0x3A, (byte) 0xBA, (byte) 0x5F, (byte) 0xC9,
                (byte) 0x31, (byte) 0x11, (byte) 0x5B, (byte) 0xC9,
                (byte) 0xCE, (byte) 0x01, (byte) 0x4F, (byte) 0xD7,
                (byte) 0x3D, (byte) 0x19, (byte) 0x92, (byte) 0x0E,
                (byte) 0xA3, (byte) 0xE7, (byte) 0xBA, (byte) 0x7A,
                (byte) 0x50, (byte) 0x5A, (byte) 0xBA, (byte) 0x72
            };
            assertTrue(
                "Password check failed for a correct password",
                Authentication.passwordMatches(keyFact, password, salt, hash)
            );
        }

        {
            byte[] password = "AllPlay&NoW0rkMakesJ0nnyAD0llBuoy".getBytes();
            byte[] salt =
            {
                (byte) 0x21, (byte) 0x39, (byte) 0xC9, (byte) 0x75,
                (byte) 0x9B, (byte) 0xBB, (byte) 0x93, (byte) 0x5F,
                (byte) 0x9D, (byte) 0xAB, (byte) 0x0D, (byte) 0x08,
                (byte) 0x7A, (byte) 0x4F, (byte) 0xA3, (byte) 0x9F
            };

            byte[] hash =
            {
                (byte) 0xD3, (byte) 0xFA, (byte) 0xF8, (byte) 0x82,
                (byte) 0x7E, (byte) 0x5D, (byte) 0x6D, (byte) 0xD1,
                (byte) 0xEB, (byte) 0x6C, (byte) 0xE5, (byte) 0xF7,
                (byte) 0x6E, (byte) 0x81, (byte) 0xC5, (byte) 0xF9,
                (byte) 0x25, (byte) 0xDF, (byte) 0x71, (byte) 0x07,
                (byte) 0x88, (byte) 0x19, (byte) 0x31, (byte) 0x52,
                (byte) 0xA0, (byte) 0xCF, (byte) 0x28, (byte) 0x8F,
                (byte) 0xB4, (byte) 0x68, (byte) 0x6F, (byte) 0x44,
                (byte) 0x70, (byte) 0x4E, (byte) 0x19, (byte) 0xAE,
                (byte) 0x3C, (byte) 0xF5, (byte) 0x60, (byte) 0x81,
                (byte) 0x3A, (byte) 0xBA, (byte) 0x5F, (byte) 0xC9,
                (byte) 0x31, (byte) 0x11, (byte) 0x5B, (byte) 0xC9,
                (byte) 0xCE, (byte) 0x01, (byte) 0x4F, (byte) 0xD7,
                (byte) 0x3D, (byte) 0x19, (byte) 0x92, (byte) 0x0E,
                (byte) 0xA3, (byte) 0xE7, (byte) 0xBA, (byte) 0x7A,
                (byte) 0x50, (byte) 0x5A, (byte) 0xBA, (byte) 0x72
            };
            assertFalse(
                "Password check succeeded for an incorrect password",
                Authentication.passwordMatches(keyFact, password, salt, hash)
            );
        }
    }
}
