package com.linbit;

import com.linbit.linstor.annotation.Nullable;

public class StringConv
{
    public static final String BOOLEAN_TRUE = Boolean.toString(true);
    public static final String BOOLEAN_FALSE = Boolean.toString(false);

    /**
     * Returns the boolean value represented by the String argument text or the
     * specified default value.
     *
     * If text contains a valid representation of a boolean value, the boolean value
     * represented by text is returned. Otherwise, the default value specified as
     * dfltValue is returned.
     *
     * @param text String representation of a boolean value; may be null
     * @param dfltValue Default result if text does not represent a boolean value
     * @return The boolean value represented by text, if text is a valid representation
     *     of a boolean value; otherwise the default value dfltValue
     */
    public static boolean getDfltBoolean(@Nullable String text, boolean dfltValue)
    {
        boolean result = dfltValue;
        if (text != null)
        {
            if (text.equalsIgnoreCase(BOOLEAN_TRUE))
            {
                result = true;
            }
            else
            if (text.equalsIgnoreCase(BOOLEAN_FALSE))
            {
                result = false;
            }
        }
        return result;
    }

    /**
     * Returns the byte value represented by the String argument text or the
     * specified default value.
     *
     * If text contains a valid representation of a byte value, that value
     * is returned. Otherwise, the value specified as dfltValue is returned.
     *
     * @param text String representation of a byte value
     * @param dfltValue Default result if text is not a valid representation of a byte value
     * @return The byte value represented by text, if text is a valid representation of a
     *     byte value, otherwise the default value dfltValue
     */
    public static byte getDfltByte(@Nullable String text, byte dfltValue)
    {
        byte value = dfltValue;
        if (text != null)
        {
            try
            {
                value = Byte.parseByte(text);
            }
            catch (NumberFormatException ignored)
            {
            }
        }
        return value;
    }

    /**
     * Returns the short value represented by the String argument text or the
     * specified default value.
     *
     * If text contains a valid representation of a short value, that value
     * is returned. Otherwise, the value specified as dfltValue is returned.
     *
     * @param text String representation of a short value
     * @param dfltValue Default result if text is not a valid representation of a short value
     * @return The short value represented by text, if text is a valid representation of a
     *     short value, otherwise the default value dfltValue
     */
    public static short getDfltShort(@Nullable String text, short dfltValue)
    {
        short value = dfltValue;
        if (text != null)
        {
            try
            {
                value = Short.parseShort(text);
            }
            catch (NumberFormatException ignored)
            {
            }
        }
        return value;
    }

    /**
     * Returns the int value represented by the String argument text or the
     * specified default value.
     *
     * If text contains a valid representation of a int value, that value
     * is returned. Otherwise, the value specified as dfltValue is returned.
     *
     * @param text String representation of a int value
     * @param dfltValue Default result if text is not a valid representation of a int value
     * @return The int value represented by text, if text is a valid representation of a
     *     int value, otherwise the default value dfltValue
     */
    public static int getDfltInt(@Nullable String text, int dfltValue)
    {
        int value = dfltValue;
        if (text != null)
        {
            try
            {
                value = Integer.parseInt(text);
            }
            catch (NumberFormatException ignored)
            {
            }
        }
        return value;
    }

    /**
     * Returns the long value represented by the String argument text or the
     * specified default value.
     *
     * If text contains a valid representation of a long value, that value
     * is returned. Otherwise, the value specified as dfltValue is returned.
     *
     * @param text String representation of a long value
     * @param dfltValue Default result if text is not a valid representation of a long value
     * @return The long value represented by text, if text is a valid representation of a
     *     long value, otherwise the default value dfltValue
     */
    public static long getDfltLong(@Nullable String text, long dfltValue)
    {
        long value = dfltValue;
        if (text != null)
        {
            try
            {
                value = Long.parseLong(text);
            }
            catch (NumberFormatException ignored)
            {
            }
        }
        return value;
    }

    /**
     * Returns the float value represented by the String argument text or the
     * specified default value.
     *
     * If text contains a valid representation of a float value, that value
     * is returned. Otherwise, the value specified as dfltValue is returned.
     *
     * @param text String representation of a float value
     * @param dfltValue Default result if text is not a valid representation of a float value
     * @return The float value represented by text, if text is a valid representation of a
     *     float value, otherwise the default value dfltValue
     */
    public static float getDfltFloat(@Nullable String text, float dfltValue)
    {
        float value = dfltValue;
        if (text != null)
        {
            try
            {
                value = Float.parseFloat(text);
            }
            catch (NumberFormatException ignored)
            {
            }
        }
        return value;
    }

    /**
     * Returns the double value represented by the String argument text or the
     * specified default value.
     *
     * If text contains a valid representation of a double value, that value
     * is returned. Otherwise, the value specified as dfltValue is returned.
     *
     * @param text String representation of a double value
     * @param dfltValue Default result if text is not a valid representation of a double value
     * @return The double value represented by text, if text is a valid representation of a
     *     double value, otherwise the default value dfltValue
     */
    public static double getDfltDouble(@Nullable String text, double dfltValue)
    {
        double value = dfltValue;
        if (text != null)
        {
            try
            {
                value = Double.parseDouble(text);
            }
            catch (NumberFormatException ignored)
            {
            }
        }
        return value;
    }

    private StringConv()
    {
    }
}
