package com.linbit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * Functions for automatic indentation of text lines
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class AutoIndent
{
    public static final int DEFAULT_INDENTATION = 4;

    private static final int CACHED_SPACER_SIZE = 40;

    private static final byte[] CACHED_SPACER = new byte[CACHED_SPACER_SIZE];
    static
    {
        Arrays.fill(CACHED_SPACER, (byte) ' ');
    }

    public static void printWithIndent(
        final PrintStream output,
        final int indent,
        final String text
    )
    {
        // PrintStream catches IOExceptions internally, so they can be caught and ignored
        try
        {
            streamIndentImpl(output, indent, text.getBytes());
        }
        catch (IOException ignored)
        {
        }
    }

    public static String formatWithIndent(final int indent, final String text)
    {
        // ByteArrayOutputStream does not throw IOExceptions on write() or flush(),
        // so they can be caught and ignored here
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try
        {
            streamIndentImpl(output, indent, text.getBytes());
        }
        catch (IOException ignored)
        {
        }
        return output.toString();
    }

    private static void streamIndentImpl(
        final OutputStream output,
        final int indent,
        final byte[] data
    )
        throws IOException
    {
        byte[] spacer;
        if (indent <= CACHED_SPACER_SIZE)
        {
            spacer = CACHED_SPACER;
        }
        else
        {
            spacer = new byte[indent];
            Arrays.fill(spacer, (byte) ' ');
        }
        int offset = 0;
        for (int index = 0; index < data.length; ++index)
        {
            if (data[index] == '\n')
            {
                if (index > offset)
                {
                    output.write(spacer, 0, indent);
                    output.write(data, offset, index - offset);
                }
                output.write('\n');
                offset = index + 1;
            }
        }
        if (offset < data.length)
        {
            output.write(spacer, 0, indent);
            output.write(data, offset, data.length - offset);
            output.write('\n');
        }
        output.flush();
    }

    private AutoIndent()
    {
    }
}
