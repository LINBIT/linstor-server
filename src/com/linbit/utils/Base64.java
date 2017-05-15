package com.linbit.utils;

public class Base64
{
    private static final char[] ALPHABET = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/', '='
    };

    // based on https://en.wikipedia.org/wiki/Base64
    public static String encode(byte[] input)
    {
        StringBuilder out = new StringBuilder((input.length * 4) / 3);
        int b;
        for (int i = 0; i < input.length; i += 3)
        {
            b = (input[i] & 0xFC) >> 2;
            out.append(ALPHABET[b]);
            b = (input[i] & 0x03) << 4;
            if (i + 1 < input.length)
            {
                b |= (input[i + 1] & 0xF0) >> 4;
                out.append(ALPHABET[b]);
                b = (input[i + 1] & 0x0F) << 2;
                if (i + 2 < input.length)
                {
                    b |= (input[i + 2] & 0xC0) >> 6;
                    out.append(ALPHABET[b]);
                    b = input[i + 2] & 0x3F;
                    out.append(ALPHABET[b]);
                }
                else
                {
                    out.append(ALPHABET[b]);
                    out.append('=');
                }
            }
            else
            {
                out.append(ALPHABET[b]);
                out.append("==");
            }
        }
        return out.toString();
    }

    public static byte[] decode(String input)
    {
        final int inputLength = input.length();
        if (inputLength % 4 != 0)
        {
            throw new IllegalArgumentException("Only base64 with padding input is allowed");
        }
        int decodedLen = (inputLength * 3) / 4;
        final int paddingIndex = input.indexOf('=');
        if (paddingIndex != -1)
        {
            decodedLen -= inputLength - paddingIndex;
        }

        final byte[] decoded = new byte[decodedLen];
        final char[] in = input.toCharArray();
        int j = 0;
        int b[] = new int[4];
        for (int i = 0; i < in.length; i += 4)
        {
            b[0] = getAlphabetIndex(in[i]);
            b[1] = getAlphabetIndex(in[i + 1]);
            b[2] = getAlphabetIndex(in[i + 2]);
            b[3] = getAlphabetIndex(in[i + 3]);
            decoded[j++] = (byte) ((b[0] << 2) | (b[1] >> 4));
            if (b[2] < 64)
            {
                decoded[j++] = (byte) ((b[1] << 4) | (b[2] >> 2));
                if (b[3] < 64)
                {
                    decoded[j++] = (byte) ((b[2] << 6) | (b[3]));
                }
            }
        }
        return decoded;
    }

    private static int getAlphabetIndex(char c)
    {
        int idx = -1;
        switch(c)
        {
            case 'A': idx = 0; break;
            case 'B': idx = 1; break;
            case 'C': idx = 2; break;
            case 'D': idx = 3; break;
            case 'E': idx = 4; break;
            case 'F': idx = 5; break;
            case 'G': idx = 6; break;
            case 'H': idx = 7; break;
            case 'I': idx = 8; break;
            case 'J': idx = 9; break;
            case 'K': idx = 10; break;
            case 'L': idx = 11; break;
            case 'M': idx = 12; break;
            case 'N': idx = 13; break;
            case 'O': idx = 14; break;
            case 'P': idx = 15; break;
            case 'Q': idx = 16; break;
            case 'R': idx = 17; break;
            case 'S': idx = 18; break;
            case 'T': idx = 19; break;
            case 'U': idx = 20; break;
            case 'V': idx = 21; break;
            case 'W': idx = 22; break;
            case 'X': idx = 23; break;
            case 'Y': idx = 24; break;
            case 'Z': idx = 25; break;
            case 'a': idx = 26; break;
            case 'b': idx = 27; break;
            case 'c': idx = 28; break;
            case 'd': idx = 29; break;
            case 'e': idx = 30; break;
            case 'f': idx = 31; break;
            case 'g': idx = 32; break;
            case 'h': idx = 33; break;
            case 'i': idx = 34; break;
            case 'j': idx = 35; break;
            case 'k': idx = 36; break;
            case 'l': idx = 37; break;
            case 'm': idx = 38; break;
            case 'n': idx = 39; break;
            case 'o': idx = 40; break;
            case 'p': idx = 41; break;
            case 'q': idx = 42; break;
            case 'r': idx = 43; break;
            case 's': idx = 44; break;
            case 't': idx = 45; break;
            case 'u': idx = 46; break;
            case 'v': idx = 47; break;
            case 'w': idx = 48; break;
            case 'x': idx = 49; break;
            case 'y': idx = 50; break;
            case 'z': idx = 51; break;
            case '0': idx = 52; break;
            case '1': idx = 53; break;
            case '2': idx = 54; break;
            case '3': idx = 55; break;
            case '4': idx = 56; break;
            case '5': idx = 57; break;
            case '6': idx = 58; break;
            case '7': idx = 59; break;
            case '8': idx = 60; break;
            case '9': idx = 61; break;
            case '+': idx = 62; break;
            case '/': idx = 63; break;
            case '=': idx = 64; break;
        }
        return idx;
    }


    public static void main(String[] args)
    {
        System.out.println("    private static int getAlphabetIndex(char c)");
        System.out.println("    {");
        System.out.println("        int idx = -1;");
        System.out.println("        switch(c)");
        System.out.println("        {");
        int idx = 0;
        for (char c : ALPHABET)
        {
            System.out.println("            case '"+c+"': idx = " + idx + "; break;");
            idx++;
        }
        System.out.println("        }");
        System.out.println("        return idx;");
        System.out.println("    }");

    }
}
