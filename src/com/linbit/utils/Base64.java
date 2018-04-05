package com.linbit.utils;

public class Base64
{

    private static final java.util.Base64.Encoder encoder = java.util.Base64.getEncoder();
    private static final java.util.Base64.Decoder decoder = java.util.Base64.getDecoder();

    public static String encode(byte[] inData)
    {
        return encoder.encodeToString(inData);
    }

    public static byte[] decode(String input)
        throws IllegalArgumentException
    {
        return decoder.decode(input);
    }

    private Base64()
    {
    }
}
