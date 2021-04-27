package com.linbit.linstor.utils;

import com.linbit.ImplementationError;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ByteUtils
{
    private static final String MD_SHA256 = "SHA-256";
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    public static byte[] checksumSha256(byte[] content)
    {
        byte[] ret;
        try
        {
            MessageDigest md = MessageDigest.getInstance(MD_SHA256);
            ret = md.digest(content);
        }
        catch (NoSuchAlgorithmException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    public static String bytesToHex(byte[] bytes)
    {
        if (bytes.length > Integer.MAX_VALUE >>> 1)
        {
            throw new IllegalArgumentException(
                "Input data size of " + bytes.length + " bytes is too large for " +
                "method bytesToHex"
            );
        }
        byte[] hexChars = new byte[bytes.length * 2];
        for (int idx = 0; idx < bytes.length; idx++)
        {
            int value = bytes[idx] & 0xFF;
            hexChars[idx * 2] = HEX_ARRAY[value >>> 4];
            hexChars[idx * 2 + 1] = HEX_ARRAY[value & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    public static byte[] hexToBytes(String hex)
    {
        char[] charArray = hex.toCharArray();
        byte[] ret = new byte[charArray.length / 2];
        for (int idx = 1; idx < charArray.length; idx += 2)
        {
            final int hi = Character.digit(charArray[idx - 1], 16);
            final int lo = Character.digit(charArray[idx], 16);
            if (hi == -1 || lo == -1)
            {
                throw new IllegalArgumentException("Invalid string passed to method hexToBytes: \"" + hex + "\"");
            }
            ret[idx / 2] = (byte) (hi << 4 | lo);
        }
        return ret;
    }
}
