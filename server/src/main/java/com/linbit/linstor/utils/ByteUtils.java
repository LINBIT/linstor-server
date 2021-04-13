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
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++)
        {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    public static byte[] hexToBytes(String hex)
    {
        char[] charArray = hex.toCharArray();
        byte[] ret = new byte[charArray.length / 2];
        for (int i = 0; i < charArray.length; i += 2)
        {
            ret[i / 2] = (byte) (Character.digit(charArray[i], 16) << 4 | Character.digit(charArray[i + 1], 16));
        }
        return ret;
    }
}
