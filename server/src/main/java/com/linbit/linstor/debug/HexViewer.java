package com.linbit.linstor.debug;

import java.util.Arrays;

@SuppressWarnings("checkstyle:magicnumber")
public class HexViewer
{
    public static final String DISPLAY_SPECIAL_CHARS =
    "!@#$%^&*()_+=-`~[]{};:'\"\\,./<>? ";

    private static boolean[] isDisplayChar = new boolean[256];

    static
    {
        Arrays.fill(isDisplayChar, false);
        for (int ch = 'a'; ch <= 'z'; ++ch)
        {
            isDisplayChar[ch] = true;
        }
        for (int ch = 'A'; ch <= 'Z'; ++ch)
        {
            isDisplayChar[ch] = true;
        }
        for (int ch = '0'; ch <= '9'; ++ch)
        {
            isDisplayChar[ch] = true;
        }
        for (byte spec_char : DISPLAY_SPECIAL_CHARS.getBytes())
        {
            // -128 to 127
            int value = spec_char;
            if (value < 0)
            {
                // -128 + 256 == 128 .. -1 + 256 == 255 => value == 0 .. 255
                value += 0x100;
            }
            isDisplayChar[value] = true;
        }
    }

    public static void dumpByteArray(byte[] data)
    {
        System.out.print("\u001B[0;32m");
        System.out.println("--[ Hex View ]--------------------------");
        System.out.print("\u001B[1;33m");
        System.out.printf("Data size = %9d\n", data.length);
        System.out.print("\u001B[0;32m");
        printByteArray(data);
        System.out.print("----------------------------------------");
        System.out.print("\u001B[0m");
        System.out.println();
    }

    public static void printByteArray(byte[] data)
    {
        StringBuilder plainText = new StringBuilder();
        int index = 0;
        while (index < data.length)
        {
            if (index % 8 == 0)
            {
                if (index > 0)
                {
                    System.out.printf(" | %s\u001b[0;32m\n", plainText.toString());
                    plainText.setLength(0);
                }
                System.out.printf("%08X | ", index);
            }
            else
            {
                System.out.print(" ");
            }

            int value = Byte.toUnsignedInt(data[index]);
            System.out.printf("%02X", value);

            if (isDisplayChar[value])
            {
                plainText.append("\u001b[0;33m");
                plainText.append((char) data[index]);
            }
            else
            {
                plainText.append("\u001b[0;35m?");
            }
            ++index;
        }
        if (index % 8 > 0)
        {
            int factor = 8 - (index % 8);
            char[] space = new char[factor * 3];
            Arrays.fill(space, ' ');
            System.out.print(space);
        }
        System.out.printf(" | %s\u001b[0;32m\n", plainText.toString());
    }

    public static String binaryToHexDump(final byte[] data)
    {
        StringBuilder hexDump = new StringBuilder();
        if (data.length == 0)
        {
            hexDump.append("<Zero length data>");
        }
        else
        {
            StringBuilder plainText = new StringBuilder();
            int index = 0;
            while (index < data.length)
            {
                if (index % 8 == 0)
                {
                    if (index > 0)
                    {
                        hexDump.append(" | ");
                        hexDump.append(plainText);
                        hexDump.append('\n');
                        plainText.setLength(0);
                    }
                    hexDump.append(String.format("%08X | ", index));
                }
                else
                {
                    hexDump.append(" ");
                }

                int value = Byte.toUnsignedInt(data[index]);
                hexDump.append(String.format("%02X", value));

                if (isDisplayChar[value])
                {
                    plainText.append((char) data[index]);
                }
                else
                {
                    plainText.append("?");
                }
                ++index;
            }
            if (index % 8 > 0)
            {
                int factor = 8 - (index % 8);
                char[] space = new char[factor * 3];
                Arrays.fill(space, ' ');
                hexDump.append(space);
            }
            hexDump.append(" | ");
            hexDump.append(plainText);
        }
        return hexDump.toString();
    }

    private HexViewer()
    {
    }
}
