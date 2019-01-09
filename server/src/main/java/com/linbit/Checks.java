package com.linbit;

import java.util.Arrays;
import java.util.regex.Matcher;
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
        String name,
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
    public static void hostNameCheck(String name) throws InvalidNameException
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

    public static void ipAddrCheck(String addr) throws InvalidIpAddressException
    {
        if (!isIpV4(addr) && !isIpV6(addr))
        {
            throw new InvalidIpAddressException(addr);
        }
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static boolean isIpV4(String addr)
    {
        Matcher matcher = IPv4_PATTERN.matcher(addr.trim());
        boolean ret = false;
        if (matcher.find())
        {
            ret = true;
            for (int idx = 1; idx <= 4; ++idx)
            {
                String stringVal = matcher.group(idx);
                int val = Integer.parseInt(stringVal);
                if (
                    (stringVal.length() > 1 && stringVal.startsWith("0")) ||
                    val < 0 ||
                    val > 255
                )
                {
                    ret = false;
                    break;
                }
            }
        }
        return ret;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static boolean isIpV6(String addr) throws InvalidIpAddressException
    {
        boolean ret = true;
        String[] compressedParts = addr.split("::");
        if (compressedParts.length > 2)
        {
            throw new InvalidIpAddressException("IPv6 must not contain multiple '::'. Address: " + addr);
        }
        if (compressedParts[0].startsWith(":"))
        {
            throw new InvalidIpAddressException("IPv6 must not start with single ':'. Address: " + addr);
        }
        if (compressedParts[compressedParts.length - 1].endsWith(":"))
        {
            throw new InvalidIpAddressException("IPv6 must not end with single ':'. Address: " + addr);
        }

        String[] leftParts = compressedParts[0].split(":");
        String[] rightParts = compressedParts.length == 1 ? new String[0] : compressedParts[1].split(":");

        String[] parts;
        int ipV6Count;
        if (rightParts.length > 0 && rightParts[rightParts.length - 1].contains("."))
        {
            // last part might be ipv4
            if (!isIpV4(rightParts[rightParts.length - 1]))
            {
                throw new InvalidIpAddressException(addr);
            }
            parts = new String[7];
            ipV6Count = 6;
            fillIpv6(parts, leftParts, rightParts);
        }
        else
        {
            if (leftParts[leftParts.length - 1].contains("."))
            {
                // Last part might be IPv4
                if (!isIpV4(leftParts[leftParts.length - 1]))
                {
                    throw new InvalidIpAddressException(addr);
                }
                parts = new String[7];
                ipV6Count = 6;
                fillIpv6(parts, leftParts, rightParts);
            }
            else
            {
                parts = new String[8];
                ipV6Count = 8;
                fillIpv6(parts, leftParts, rightParts);
            }
        }

        try
        {
            for (int idx = 0; idx < ipV6Count; ++idx)
            {
                String part = parts[idx];
                int val = Integer.parseInt(part, 16);
                if (val < 0 || val > 0xFFFF)
                {
                    throw new InvalidIpAddressException(addr);
                }
            }
        }
        catch (NumberFormatException nfExc)
        {
            ret = false;
        }
        return ret;
    }

    private static void fillIpv6(String[] parts, String[] leftParts, String[] rightParts)
    {
        Arrays.fill(parts, "0");
        if (leftParts.length > 1 || !leftParts[0].isEmpty())
        {
            System.arraycopy(leftParts, 0, parts, 0, leftParts.length);
        }
        System.arraycopy(rightParts, 0, parts, parts.length - rightParts.length, rightParts.length);
    }

    public static <T> T requireNonNull(T t)
    {
        if (t == null)
        {

        }
        return t;
    }

}
