package com.linbit;

import java.math.BigInteger;

import com.linbit.utils.BigIntegerUtils;

/**
 * Methods for converting decimal and binary sizes of different magnitudes
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SizeConv
{
    public static final BigInteger one = BigInteger.valueOf(1L);

    public enum SizeUnit
    {
        UNIT_B,
        UNIT_kiB,
        UNIT_MiB,
        UNIT_GiB,
        UNIT_TiB,
        UNIT_PiB,
        UNIT_EiB,
        UNIT_ZiB,
        UNIT_YiB,
        UNIT_kB,
        UNIT_MB,
        UNIT_GB,
        UNIT_TB,
        UNIT_PB,
        UNIT_EB,
        UNIT_ZB,
        UNIT_YB;

        protected BigInteger getFactor()
        {
            BigInteger factor = FACTOR_B;
            switch (this)
            {
                case UNIT_B:
                    factor = FACTOR_B;
                    break;
                case UNIT_kiB:
                    factor = FACTOR_kiB;
                    break;
                case UNIT_MiB:
                    factor = FACTOR_MiB;
                    break;
                case UNIT_GiB:
                    factor = FACTOR_GiB;
                    break;
                case UNIT_TiB:
                    factor = FACTOR_TiB;
                    break;
                case UNIT_PiB:
                    factor = FACTOR_PiB;
                    break;
                case UNIT_EiB:
                    factor = FACTOR_EiB;
                    break;
                case UNIT_ZiB:
                    factor = FACTOR_ZiB;
                    break;
                case UNIT_YiB:
                    factor = FACTOR_YiB;
                    break;
                case UNIT_kB:
                    factor = FACTOR_kB;
                    break;
                case UNIT_MB:
                    factor = FACTOR_MB;
                    break;
                case UNIT_GB:
                    factor = FACTOR_GB;
                    break;
                case UNIT_TB:
                    factor = FACTOR_TB;
                    break;
                case UNIT_PB:
                    factor = FACTOR_PB;
                    break;
                case UNIT_EB:
                    factor = FACTOR_EB;
                    break;
                case UNIT_ZB:
                    factor = FACTOR_ZB;
                    break;
                case UNIT_YB:
                    factor = FACTOR_YB;
                    break;
                default:
                    throw new ImplementationError(
                        String.format(
                            "Missing case label for enum member %s",
                            this.name()
                        ),
                        null
                    );
            }
            return factor;
        }
    }

    // Factor 1
    public static final BigInteger FACTOR_B   = BigInteger.valueOf(1L);

    // Factor 1,024
    public static final BigInteger FACTOR_kiB = BigInteger.valueOf(1024L);

    // Factor 1,048,576
    public static final BigInteger FACTOR_MiB = BigInteger.valueOf(1048576L);

    // Factor 1,073,741,824
    public static final BigInteger FACTOR_GiB = BigInteger.valueOf(1073741824L);

    // Factor 1,099,511,627,776
    public static final BigInteger FACTOR_TiB = BigInteger.valueOf(0x10000000000L);

    // Factor 1,125,899,906,842,624
    public static final BigInteger FACTOR_PiB = BigInteger.valueOf(0x4000000000000L);

    // Factor 1,152,921,504,606,846,976
    public static final BigInteger FACTOR_EiB = BigInteger.valueOf(0x1000000000000000L);

    // Factor 1,180,591,620,717,411,303,424
    public static final BigInteger FACTOR_ZiB = new BigInteger(
        new byte[] { 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }
    );

    // Factor 1,208,925,819,614,629,174,706,176
    public static final BigInteger FACTOR_YiB = new BigInteger(
        new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }
    );

    // Factor 1,000
    public static final BigInteger FACTOR_kB = BigInteger.valueOf(1000L);

    // Factor 1,000,000
    public static final BigInteger FACTOR_MB = BigInteger.valueOf(1000000L);

    // Factor 1,000,000,000
    public static final BigInteger FACTOR_GB = BigInteger.valueOf(1000000000L);

    // Factor 1,000,000,000,000
    public static final BigInteger FACTOR_TB = BigInteger.valueOf(1000000000000L);

    // Factor 1,000,000,000,000,000
    public static final BigInteger FACTOR_PB = BigInteger.valueOf(1000000000000000L);

    // Factor 1,000,000,000,000,000,000
    public static final BigInteger FACTOR_EB = BigInteger.valueOf(1000000000000000000L);

    // Factor 1,000,000,000,000,000,000,000
    public static final BigInteger FACTOR_ZB = new BigInteger(
        new byte[]
        {
            0x36, 0x35, (byte) 0xC9, (byte) 0xAD,
            (byte) 0xC5, (byte) 0xDE, (byte) 0xA0,
            0x00, 0x00
        }
    );

    // Factor 1,000,000,000,000,000,000,000,000
    public static final BigInteger FACTOR_YB = new BigInteger(
        new byte[]
        {
            0x00, (byte) 0xD3, (byte) 0xC2, 0x1B,
            (byte) 0xCE, (byte) 0xCC, (byte) 0xED, (byte) 0xA1,
            0x00, 0x00, 0x00
        }
    );

    public static long convert(long size, SizeUnit unitIn, SizeUnit unitOut)
    {
        BigInteger convert = convert(BigInteger.valueOf(size), unitIn, unitOut);
        return BigIntegerUtils.longValueExact(convert);
    }

    public static long convertRoundUp(long size, SizeUnit unitIn, SizeUnit unitOut)
    {
        BigInteger convert= convertRoundUp(BigInteger.valueOf(size), unitIn, unitOut);
        return BigIntegerUtils.longValueExact(convert);
    }

    public static BigInteger convert(BigInteger size, SizeUnit unitIn, SizeUnit unitOut)
    {
        BigInteger factorIn = unitIn.getFactor();
        BigInteger divisorOut = unitOut.getFactor();
        BigInteger result = size.multiply(factorIn);
        result = result.divide(divisorOut);
        return result;
    }

    public static BigInteger convertRoundUp(BigInteger size, SizeUnit unitIn, SizeUnit unitOut)
    {
        BigInteger factorIn = unitIn.getFactor();
        BigInteger divisorOut = unitOut.getFactor();
        BigInteger result = size.multiply(factorIn);
        BigInteger[] quotients = result.divideAndRemainder(divisorOut);
        result = quotients[0];
        // Quick check for remainder > 0; if any bits are set, the remainder is not zero
        if (quotients[1].bitLength() > 0)
        {
            result = result.add(one);
        }
        return result;
    }
}
