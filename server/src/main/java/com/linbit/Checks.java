package com.linbit;

import com.linbit.linstor.annotation.Nullable;

import java.util.regex.Pattern;

/**
 * Universal validity checks
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Checks
{
    // Naming convention exception: IPv4 capitalization
    @SuppressWarnings("checkstyle:constantname")
    private static final Pattern IPv4_PATTERN = Pattern.compile(
        "^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$"
    );

    public static final int HOSTNAME_MIN_LENGTH = 2;
    public static final int HOSTNAME_MAX_LENGTH = 255;
    public static final int HOSTNAME_LABEL_MAX_LENGTH = 63;

    private static final String RANGE_EXC_FORMAT =
        "Value %d is out of range [%d - %d]";

    private Checks()
    {
    }

    /**
     * Universal name check
     *
     * @param name The name to check for validity
     * @param minLength Allowed minimum length of the name
     * @param maxLength Allowed maximum length of the name
     * @param validChars Letters allowed anywhere in the name
     * @param validInnerChars Letters allowed after the first letter in the name
     * @throws InvalidNameException If the name is not valid
     */
    public static void nameCheck(
        @Nullable String name,
        int minLength,
        int maxLength,
        byte[] validChars,
        byte[] validInnerChars
    ) throws InvalidNameException
    {
        if (name == null)
        {
           throw new ImplementationError(
               "Method called with name == null",
               new NullPointerException()
           );
        }

        byte[] nameBuffer = name.getBytes();

        if (minLength < 1)
        {
            throw new ImplementationError(
                "Method called with minLength < 1",
                new IllegalArgumentException()
            );
        }

        // Length check
        if (nameBuffer.length < minLength)
        {
            throw new InvalidNameException(
                String.format(
                    "Invalid name: Name length %d is less than minimum length %d",
                    nameBuffer.length, minLength
                ),
                name
            );
        }
        if (nameBuffer.length > maxLength)
        {
            throw new InvalidNameException(
                String.format(
                    "Invalid name: Name length %d is greater than maximum length %d",
                    nameBuffer.length, maxLength
                ),
                name
            );
        }

        // Check for the presence of alpha-numeric characters
        {
            boolean alpha = false;
            for (int idx = 0; idx < nameBuffer.length; ++idx)
            {
                byte letter = nameBuffer[idx];
                if ((letter >= 'a' && letter <= 'z') ||
                    (letter >= 'A' && letter <= 'Z'))
                {
                    alpha = true;
                    break;
                }
            }
            if (!alpha)
            {
                throw new InvalidNameException(
                    "Invalid name: Name must contain at least one character that matches [a-zA-Z]",
                    name
                );
            }
        }

        // First character validity check
        {
            byte letter = nameBuffer[0];
            if (!((letter >= 'a' && letter <= 'z') ||
                (letter >= 'A' && letter <= 'Z')))
            {
                int vIdx = 0;
                while (vIdx < validChars.length)
                {
                    if (letter == validChars[vIdx])
                    {
                        break;
                    }
                    ++vIdx;
                }
                if (vIdx >= validChars.length)
                {
                    throw new InvalidNameException(
                        String.format(
                            "Invalid name: Cannot begin with character '%c'",
                            (char) letter
                        ),
                        name
                    );
                }
            }
        }

        // Remaining characters validity check
        for (int idx = 1; idx < nameBuffer.length; ++idx)
        {
            byte letter = nameBuffer[idx];
            if (!((letter >= 'a' && letter <= 'z') ||
                (letter >= 'A' && letter <= 'Z') ||
                (letter >= '0' && letter <= '9')))
            {
                int vIdx = 0;
                while (vIdx < validInnerChars.length)
                {
                    if (letter == validInnerChars[vIdx])
                    {
                        break;
                    }
                    ++vIdx;
                }
                if (vIdx >= validInnerChars.length)
                {
                    throw new InvalidNameException(
                        String.format(
                            "Invalid name: Cannot contain character '%c'",
                            (char) letter
                        ),
                        name
                    );
                }
            }
        }
    }

    /**
     * RFC952 / RFC1035 / RFC1123 internet host name validity check
     *
     * @param name The hostname to check for validity
     * @throws InvalidNameException If the hostname is not valid
     */
    public static void hostNameCheck(@Nullable String name) throws InvalidNameException
    {
        if (name == null)
        {
            throw new ImplementationError(
                "Method called with name == null",
                new NullPointerException()
            );
        }

        // First & last character special treatment check
        if (name.startsWith("."))
        {
            throw new InvalidNameException(
                "Hostname cannot begin with '.'",
                name
            );
        }
        if (name.startsWith("-"))
        {
            throw new InvalidNameException(
                "Hostname cannot begin with '-'",
                name
            );
        }
        if (name.endsWith("-"))
        {
            throw new InvalidNameException(
                "Hostname cannot end with '-'",
                name
            );
        }

        byte[] nameBuffer = name.getBytes();

        // Length check
        if (nameBuffer.length < HOSTNAME_MIN_LENGTH)
        {
            throw new InvalidNameException(
                String.format(
                    "Hostname length of %d violates RFC1123 minimum length of %d",
                    nameBuffer.length, HOSTNAME_MIN_LENGTH
                ),
                name
            );
        }
        if (nameBuffer.length > HOSTNAME_MAX_LENGTH)
        {
            throw new InvalidNameException(
                String.format(
                    "Hostname length of %d violates RFC1123 maximum length of %d",
                    nameBuffer.length, HOSTNAME_MAX_LENGTH
                ),
                name
            );
        }

        // Individual domain name components length check
        {
            int labelLength = 0;
            for (int idx = 0; idx < nameBuffer.length; ++idx)
            {
                byte letter = nameBuffer[idx];
                if (letter == '.')
                {
                    labelLength = 0;
                }
                else
                {
                    ++labelLength;
                    if (labelLength > HOSTNAME_LABEL_MAX_LENGTH)
                    {
                        throw new InvalidNameException(
                            String.format(
                                "Domain name component length of %d violates RFC1123 maximum length of %d",
                                labelLength, HOSTNAME_LABEL_MAX_LENGTH
                            ),
                            name
                        );
                    }
                    if (!((letter >= 'a' && letter <= 'z') ||
                        (letter >= 'A' && letter <= 'Z') ||
                        (letter >= '0' && letter <= '9') ||
                        (letter == '.' || letter == '-')))
                    {
                        throw new InvalidNameException(
                            String.format(
                                "Domain name cannot contain character '%c'",
                                (char) letter
                            ),
                            name
                        );
                    }
                }
            }
        }
    }

    /**
     * Checks whether a value is within the range [minValue, maxValue]
     *
     * @param value The value to check
     * @param minValue Allowed minimum value
     * @param maxValue Allowed maximum value
     * @throws ValueOutOfRangeException If the value is out of range
     */
    public static void rangeCheck(long value, long minValue, long maxValue)
        throws ValueOutOfRangeException
    {
        genericRangeCheck(value, minValue, maxValue, RANGE_EXC_FORMAT);
    }

    /**
     * Checks whether a value is within the range [minValue, maxValue], and
     * uses the specified format to generate an exception with a message
     * describing the problem
     *
     * @param value The value to check
     * @param minValue Allowed minimum value
     * @param maxValue Allowed maximum value
     * @throws ValueOutOfRangeException If the value is out of range
     */
    public static void genericRangeCheck(
        long value,
        long minValue, long maxValue,
        String excFormat
    ) throws ValueOutOfRangeException
    {
        if (minValue > maxValue)
        {
            throw new ImplementationError(
                String.format(
                    "Impossible range: Method called with minValue %d > maxValue %d",
                    minValue, maxValue
                ),
                new IllegalArgumentException()
            );
        }

        if (value < minValue)
        {
            throw new ValueOutOfRangeException(
                String.format(
                    excFormat,
                    value, minValue, maxValue
                ),
                ValueOutOfRangeException.ViolationType.TOO_LOW
            );
        }
        if (value > maxValue)
        {
            throw new ValueOutOfRangeException(
                String.format(
                    excFormat,
                    value, minValue, maxValue
                ),
                ValueOutOfRangeException.ViolationType.TOO_HIGH
            );
        }
    }
}
