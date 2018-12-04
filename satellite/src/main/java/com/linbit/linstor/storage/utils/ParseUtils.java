package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class ParseUtils
{
    /**
     * Parse a decimal that may use ',' or '.' as decimal separator but does not contain any grouping separators.
     * Returns the integer part.
     *
     * @throws NumberFormatException
     */
    public static long parseDecimalAsLong(String raw)
    {
        // Ignore the fractional part, but throw an exception if the value is too large.
        return parseDecimal(raw).toBigInteger().longValueExact();
    }

    /**
     * Parse a decimal that may use ',' or '.' as decimal separator but does not contain any grouping separators.
     *
     * @throws NumberFormatException
     */
    public static BigDecimal parseDecimal(String raw)
    {
        return new BigDecimal(raw.replace(',', '.'));
    }

    public static Map<String, Long> parseSimpleTable(OutputData output, String delimiter, String descr)
        throws StorageException
    {
        final int expectedColums = 2;

        final Map<String, Long> result = new HashMap<>();

        final String stdOut = new String(output.stdoutData);
        final String[] lines = stdOut.split("\n");

        for (final String line : lines)
        {
            final String[] data = line.trim().split(delimiter);
            if (data.length == expectedColums)
            {
                try
                {
                    result.put(
                        data[0],
                        StorageUtils.parseDecimalAsLong(data[1])
                    );
                }
                catch (NumberFormatException nfExc)
                {
                    throw new StorageException(
                        "Unable to parse '" + descr + "'",
                        "Numeric value to parse: '" + data[1] + "'",
                        null,
                        null,
                        "External command: " + String.join(" ", output.executedCommand),
                        nfExc
                    );
                }
            }
            else
            {
                throw new StorageException(
                    "Unable to parse '" + descr + "'",
                    "Expected " + expectedColums + " columns, but got " + data.length,
                    "Failed to parse line: '" + line + "'",
                    null,
                    "External command: " + String.join(" ", output.executedCommand)
                );
            }
        }
        return result;
    }

}
