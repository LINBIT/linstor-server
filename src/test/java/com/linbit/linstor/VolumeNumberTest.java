package com.linbit.linstor;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.VolumeNumber;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeNumberTest
{
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int[] CTOR_FAIL_TESTS =
    {
        VolumeNumber.VOLUME_NR_MIN - 1,
        Integer.MIN_VALUE,
        VolumeNumber.VOLUME_NR_MAX + 1,
        Integer.MAX_VALUE
    };

    public static final int[] TO_STRING_TESTS =
    {
        0,
        1,
        677,
        32767
    };

    VolumeNumber refNr;
    VolumeNumber testLess;
    VolumeNumber testGreater;
    VolumeNumber testEqual;

    @SuppressWarnings("checkstyle:magicnumber")
    public VolumeNumberTest()
        throws ValueOutOfRangeException
    {
        refNr = new VolumeNumber(19910);
        testLess = new VolumeNumber(0);
        testGreater = new VolumeNumber(19911);
        testEqual = new VolumeNumber(19910);
    }

    @SuppressWarnings("unused")
    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
    @Test
    public void ctorTests()
        throws ValueOutOfRangeException
    {
        for (int invalidNr : CTOR_FAIL_TESTS)
        {
            try
            {
                VolumeNumber tmp = new VolumeNumber(invalidNr);
                fail(String.format("Constructor succeeded with volume number = %d", invalidNr));
            }
            catch (ValueOutOfRangeException expectedExc)
            {
                // Construction failed as expected, nothing to do
            }
        }
        if (VolumeNumber.VOLUME_NR_MIN >= VolumeNumber.VOLUME_NR_MAX)
        {
            fail(
                String.format(
                    "Bad constants, VOLUME_NR_MIN (%d) >= VOLUME_NR_MAX (%d)",
                    VolumeNumber.VOLUME_NR_MIN, VolumeNumber.VOLUME_NR_MAX
                )
            );
        }
        int validNr = VolumeNumber.VOLUME_NR_MIN;
        while (validNr <= VolumeNumber.VOLUME_NR_MAX)
        {
            VolumeNumber tmp = new VolumeNumber(validNr);
            if (tmp.value != validNr)
            {
                fail(
                    String.format(
                        "VolumeNumber value %d != construction volume number %d",
                        tmp.value, validNr
                    )
                );
            }
            // Avoid overflow if VOLUME_NR_MAX == Integer.MAX_VALUE
            if (validNr >= VolumeNumber.VOLUME_NR_MAX)
            {
                break;
            }
            ++validNr;
        }
    }

    @Test
    public void testCompareTo()
    {
        // Compare to same instance
        if (refNr.compareTo(refNr) != 0)
        {
            fail("Comparison with same instance failed");
        }

        if (refNr.compareTo(testLess) <= 0)
        {
            fail("Comparison with lower number failed");
        }

        if (refNr.compareTo(testGreater) >= 0)
        {
            fail("Comparison with greater number failed");
        }

        if (refNr.compareTo(testEqual) != 0 || testEqual.compareTo(refNr) != 0)
        {
            fail("Comparison with equal number failed");
        }
    }

    @Test
    public void testEquals()
    {
        if (!refNr.equals(refNr))
        {
            fail("Equality check failed for same instance");
        }

        if (!refNr.equals(testEqual) || !testEqual.equals(refNr))
        {
            fail("Equality check failed for equal number");
        }

        if (refNr.equals(testLess) || refNr.equals(testGreater))
        {
            fail("Equality check reported equal for an unequal number");
        }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testHashCode()
        throws ValueOutOfRangeException
    {
        // Hash code generation normally uses multiplication with prime numbers
        VolumeNumber nrOne = new VolumeNumber(4);
        VolumeNumber nrTwo = new VolumeNumber(31);

        // Create another VolumeNumber object with the same value as the first one.
        // This should have the same hash value as the first one.
        VolumeNumber nrThree = new VolumeNumber(4);

        int hashOne = nrOne.hashCode();
        int hashTwo = nrTwo.hashCode();
        int hashThree = nrThree.hashCode();

        if (hashOne == hashTwo)
        {
            fail(
                String.format(
                    "hashCode() for volume numbers %d and %d is equal (%d)",
                    nrOne.value, nrTwo.value, hashOne
                )
            );
        }
        if (hashOne != hashThree)
        {
            fail(
                String.format(
                    "hashCode() for two equal port numbers (%d) differs (%d vs %d)",
                    nrOne.value, hashOne, hashThree
                )
            );
        }
    }

    @Test
    public void testToString()
        throws ValueOutOfRangeException, NumberFormatException
    {
        for (int nr : TO_STRING_TESTS)
        {
            VolumeNumber tmp = new VolumeNumber(nr);
            String nrStr = tmp.toString();
            int checkNr = Integer.parseInt(nrStr);
            if (checkNr != nr)
            {
                fail(
                    String.format(
                        "VolumeNumber toString() generated string \"%s\", which parsed to value %d instead of %d",
                        nrStr, checkNr, nr
                    )
                );
            }
        }
    }
}
