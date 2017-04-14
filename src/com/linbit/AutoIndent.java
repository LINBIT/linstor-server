package com.linbit;

import java.io.PrintStream;
import java.util.Arrays;

/**
 * Functions for automatic indentation of text lines
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class AutoIndent
{
    public static void printWithIndent(
        PrintStream output,
        int indent,
        String text
    )
    {
        byte[] spacer = new byte[indent];
        Arrays.fill(spacer, (byte) ' ');
        byte[] data = text.getBytes();
        int offset = 0;
        for (int index = 0; index < data.length; ++index)
        {
            if (data[index] == '\n')
            {
                if (index > offset)
                {
                    output.write(spacer, 0, spacer.length);
                    output.write(data, offset, index - offset);
                }
                output.println();
                offset = index + 1;
            }
        }
        if (offset < data.length)
        {
            output.write(spacer, 0, spacer.length);
            output.write(data, offset, data.length - offset);
            output.println();
        }
    }
}
