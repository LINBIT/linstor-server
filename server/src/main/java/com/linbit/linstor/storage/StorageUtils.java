package com.linbit.linstor.storage;

import java.math.BigDecimal;

public class StorageUtils
{
    // DO NOT USE "," or "." AS DELIMITER due to localization issues
    public static final String DELIMITER = ";";

    /**
     * Parse a decimal that may use ',' or '.' as decimal separator but does not contain any grouping separators.
     *
     * @throws NumberFormatException
     */
    public static BigDecimal parseDecimal(String raw) throws NumberFormatException
    {
        return new BigDecimal(raw.replace(',', '.'));
    }

    /**
     * Parse a decimal that may use ',' or '.' as decimal separator but does not contain any grouping separators.
     * Returns the integer part.
     *
     * @throws NumberFormatException
     */
    public static long parseDecimalAsLong(String raw) throws NumberFormatException
    {
        // Ignore the fractional part, but throw an exception if the value is too large.
        return parseDecimal(raw).toBigInteger().longValueExact();
    }

    /**
     * Parse a decimal that may use ',' or '.' as decimal separator but does not contain any grouping separators.
     * Returns the integer part.
     *
     * @throws NumberFormatException
     */
    public static int parseDecimalAsInt(String raw) throws NumberFormatException
    {
        // Ignore the fractional part, but throw an exception if the value is too large.
        return parseDecimal(raw).toBigInteger().intValueExact();
    }

    public static float parseDecimalAsFloat(String raw) throws NumberFormatException
    {
        return parseDecimal(raw).floatValue();
    }

    private StorageUtils()
    {
    }
}
