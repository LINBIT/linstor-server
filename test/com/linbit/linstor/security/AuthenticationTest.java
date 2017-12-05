package com.linbit.linstor.security;

import java.security.MessageDigest;
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
    public void testPasswordsMatch() throws Exception
    {
        MessageDigest md = MessageDigest.getInstance("SHA-512");
        {
            byte[] password = "AllW0rk&NoPlayMakesJ0nnyADullB0y".getBytes();
            byte[] salt =
            {
                (byte) 0x27, (byte) 0x23, (byte) 0xB0, (byte) 0x2F,
                (byte) 0x4B, (byte) 0x8F, (byte) 0x0F, (byte) 0xA5,
                (byte) 0xD8, (byte) 0x79, (byte) 0xB1, (byte) 0x6B,
                (byte) 0x1F, (byte) 0xEF, (byte) 0x21, (byte) 0x9F
            };

            byte[] hash =
            {
                (byte) 0x05, (byte) 0xDC, (byte) 0xA8, (byte) 0x44,
                (byte) 0x9C, (byte) 0xBC, (byte) 0xD5, (byte) 0xCB,
                (byte) 0x52, (byte) 0x48, (byte) 0x0E, (byte) 0xE6,
                (byte) 0x63, (byte) 0xD1, (byte) 0xFE, (byte) 0xD8,
                (byte) 0xD4, (byte) 0x20, (byte) 0xF1, (byte) 0x4E,
                (byte) 0x59, (byte) 0xFC, (byte) 0xDD, (byte) 0xFF,
                (byte) 0x13, (byte) 0x04, (byte) 0xE2, (byte) 0xDF,
                (byte) 0x40, (byte) 0x31, (byte) 0xF7, (byte) 0x5B,
                (byte) 0xD1, (byte) 0xEE, (byte) 0x7F, (byte) 0x2F,
                (byte) 0x4F, (byte) 0xF6, (byte) 0xF4, (byte) 0x2C,
                (byte) 0xCE, (byte) 0x74, (byte) 0xF7, (byte) 0x87,
                (byte) 0x5A, (byte) 0x3A, (byte) 0xD8, (byte) 0x86,
                (byte) 0x3B, (byte) 0x7D, (byte) 0xBF, (byte) 0xAA,
                (byte) 0x60, (byte) 0xA3, (byte) 0x4B, (byte) 0xA4,
                (byte) 0xD7, (byte) 0xD0, (byte) 0x7F, (byte) 0xAA,
                (byte) 0xBB, (byte) 0x89, (byte) 0x10, (byte) 0x7A
            };
            assertTrue(
                "Password check failed for a correct password",
                Authentication.passwordMatches(md, password, salt, hash)
            );
        }

        {
            byte[] password = "AllPlay&NoW0rkMakesJ0nnyADullB0y".getBytes();
            byte[] salt =
            {
                (byte) 0x27, (byte) 0x23, (byte) 0xB0, (byte) 0x2F,
                (byte) 0x4B, (byte) 0x8F, (byte) 0x0F, (byte) 0xA5,
                (byte) 0xD8, (byte) 0x79, (byte) 0xB1, (byte) 0x6B,
                (byte) 0x1F, (byte) 0xEF, (byte) 0x21, (byte) 0x9F
            };

            byte[] hash =
            {
                (byte) 0x05, (byte) 0xDC, (byte) 0xA8, (byte) 0x44,
                (byte) 0x9C, (byte) 0xBC, (byte) 0xD5, (byte) 0xCB,
                (byte) 0x52, (byte) 0x48, (byte) 0x0E, (byte) 0xE6,
                (byte) 0x63, (byte) 0xD1, (byte) 0xFE, (byte) 0xD8,
                (byte) 0xD4, (byte) 0x20, (byte) 0xF1, (byte) 0x4E,
                (byte) 0x59, (byte) 0xFC, (byte) 0xDD, (byte) 0xFF,
                (byte) 0x13, (byte) 0x04, (byte) 0xE2, (byte) 0xDF,
                (byte) 0x40, (byte) 0x31, (byte) 0xF7, (byte) 0x5B,
                (byte) 0xD1, (byte) 0xEE, (byte) 0x7F, (byte) 0x2F,
                (byte) 0x4F, (byte) 0xF6, (byte) 0xF4, (byte) 0x2C,
                (byte) 0xCE, (byte) 0x74, (byte) 0xF7, (byte) 0x87,
                (byte) 0x5A, (byte) 0x3A, (byte) 0xD8, (byte) 0x86,
                (byte) 0x3B, (byte) 0x7D, (byte) 0xBF, (byte) 0xAA,
                (byte) 0x60, (byte) 0xA3, (byte) 0x4B, (byte) 0xA4,
                (byte) 0xD7, (byte) 0xD0, (byte) 0x7F, (byte) 0xAA,
                (byte) 0xBB, (byte) 0x89, (byte) 0x10, (byte) 0x7A
            };
            assertFalse(
                "Password check succeeded for an incorrect password",
                Authentication.passwordMatches(md, password, salt, hash)
            );
        }
    }
}
