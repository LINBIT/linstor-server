package com.linbit.utils;

import java.util.Arrays;

public class Base64
{
    // Base64 encode table
    private static final byte[] ALPHABET =
    {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/', '='
    };

    // Base64 decode table
    private static final byte[] REVERSE_ALPHABET = new byte[256];
    static
    {
        Arrays.fill(REVERSE_ALPHABET, (byte) -1);
        for (int idx = 0; idx < ALPHABET.length; ++idx)
        {
            int revIdx = ALPHABET[idx] >= 0 ? ALPHABET[idx] : 256 + ALPHABET[idx];
            REVERSE_ALPHABET[revIdx] = (byte) idx;
        }
    }

    private static final String INVALID_INPUT_MSG = "Base64 decode encountered an invalid input value";

    // Based on https://en.wikipedia.org/wiki/Base64
    public static String encode(byte[] inData)
    {
        byte[] outData = new byte[((inData.length + 2) / 3) * 4];
        int outIdx = 0;
        for (int inIdx = 0; inIdx < inData.length; inIdx += 3)
        {
            int b = (inData[inIdx] & 0xFC) >>> 2;
            outData[outIdx] = ALPHABET[b];
            b = (inData[inIdx] & 0x03) << 4;
            if (inIdx + 1 < inData.length)
            {
                b |= (inData[inIdx + 1] & 0xF0) >>> 4;
                outData[outIdx + 1] = ALPHABET[b];
                b = (inData[inIdx + 1] & 0x0F) << 2;
                if (inIdx + 2 < inData.length)
                {
                    b |= (inData[inIdx + 2] & 0xC0) >>> 6;
                    outData[outIdx + 2] = ALPHABET[b];
                    b = inData[inIdx + 2] & 0x3F;
                    outData[outIdx + 3] = ALPHABET[b];
                }
                else
                {
                    outData[outIdx + 2] = ALPHABET[b];
                    outData[outIdx + 3] = '=';
                }
            }
            else
            {
                outData[outIdx + 1] = ALPHABET[b];
                outData[outIdx + 2] = '=';
                outData[outIdx + 3] = '=';
            }
            outIdx += 4;
        }
        return new String(outData);
    }

    public static byte[] decode(String input)
        throws IllegalArgumentException
    {
        final int inputLength = input.length();
        if (inputLength % 4 != 0)
        {
            throw new IllegalArgumentException("Only padded Base64 input is supported");
        }
        int decodedLength = (inputLength * 3) / 4;
        final int paddingIndex = input.indexOf('=');
        if (paddingIndex != -1)
        {
            decodedLength -= inputLength - paddingIndex;
        }

        final byte[] outData = new byte[decodedLength];
        final byte[] inData = input.getBytes();
        int outIdx = 0;
        int[] buf = new int[4];
        for (int inIdx = 0; inIdx < inData.length; inIdx += 4)
        {
            buf[0] = getAlphabetIndex(inData[inIdx]);
            buf[1] = getAlphabetIndex(inData[inIdx + 1]);
            buf[2] = getAlphabetIndex(inData[inIdx + 2]);
            buf[3] = getAlphabetIndex(inData[inIdx + 3]);
            outData[outIdx] = (byte) ((buf[0] << 2) | (buf[1] >> 4));
            if (buf[2] < 64)
            {
                outData[outIdx + 1] = (byte) ((buf[1] << 4) | (buf[2] >> 2));
                if (buf[3] < 64)
                {
                    outData[outIdx + 2] = (byte) ((buf[2] << 6) | (buf[3]));
                }
            }
            outIdx += 3;
        }
        return outData;
    }

    private static int getAlphabetIndex(byte inByte)
        throws IllegalArgumentException
    {
        int revIdx = inByte >= 0 ? inByte : 256 + inByte;
        int idx;
        try
        {
            idx = REVERSE_ALPHABET[revIdx];
        }
        catch (ArrayIndexOutOfBoundsException boundsExc)
        {
            throw new IllegalArgumentException(INVALID_INPUT_MSG);
        }
        if (idx < 0)
        {
            throw new IllegalArgumentException(INVALID_INPUT_MSG);
        }
        return idx;
    }

    private Base64()
    {
    }
}
