package com.linbit.utils;

public class Base64
{
    private static final java.util.Base64.Encoder ENCODER = java.util.Base64.getEncoder();
    private static final java.util.Base64.Decoder DECODER = java.util.Base64.getDecoder();

    public static String encode(byte[] inData)
    {
        return ENCODER.encodeToString(inData);
    }

    public static byte[] decode(String input)
        throws IllegalArgumentException
    {
        return DECODER.decode(input);
    }

    private Base64()
    {
    }
}
