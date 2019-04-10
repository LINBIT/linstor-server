package com.linbit.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Base64Test
{
    @Test
    public void testStdEncode()
    {
        // testcases from http://www.rfc-base.org/txt/rfc-4648.txt
        checkEncode("", "");
        checkEncode("Zg==", "f");
        checkEncode("Zm8=", "fo");
        checkEncode("Zm9v", "foo");
        checkEncode("Zm9vYg==", "foob");
        checkEncode("Zm9vYmE=", "fooba");
        checkEncode("Zm9vYmFy", "foobar");
    }

    @Test
    public void testStdDecode()
    {
        // testcases from http://www.rfc-base.org/txt/rfc-4648.txt
        checkDecode("", "");
        checkDecode("Zg==", "f");
        checkDecode("Zm8=", "fo");
        checkDecode("Zm9v", "foo");
        checkDecode("Zm9vYg==", "foob");
        checkDecode("Zm9vYmE=", "fooba");
        checkDecode("Zm9vYmFy", "foobar");
    }

    private void checkEncode(String expectedEncodedString, String input)
    {
        assertEquals(expectedEncodedString, Base64.encode(input.getBytes()));
    }

    private void checkDecode(String encoded, String expectedOutput)
    {
        assertEquals(expectedOutput, new String(Base64.decode(encoded)));
    }

}
