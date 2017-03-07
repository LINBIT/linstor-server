package com.linbit.drbdmanage.debug;

public class HexViewer
{
    public static void dumpByteArray(byte[] data)
    {
        System.out.print("\u001B[0;32m");
        System.out.println("--[ Hex View ]--------------------------");
        System.out.printf("byte[] size = %9d\n", data.length);
        printByteArray(data);
        System.out.print("----------------------------------------");
        System.out.print("\u001B[0m");
        System.out.println();
    }

    public static void printByteArray(byte[] data)
    {
        for (int index = 0; index < data.length; ++index)
        {
            if (index % 8 == 0)
            {
                if (index > 0)
                {
                    System.out.println();
                }
            }
            else
            {
                System.out.print(" ");
            }

            int value = data[index];
            if (value < 0)
            {
                value += 0x100;
            }
            System.out.printf("0x%02X", value);
        }
        System.out.println();
    }
}
