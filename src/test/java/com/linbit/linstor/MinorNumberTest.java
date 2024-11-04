package com.linbit.linstor;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.types.MinorNumber;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MinorNumberTest
{
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int[] CTOR_FAIL_TESTS =
    {
        -1,
        Integer.MIN_VALUE,
        (1 << 20),
        Integer.MAX_VALUE
    };

    public static final int[] TO_STRING_TESTS =
    {
        0,
        1,
        750,
        997878
    };

    MinorNumber refNr;
    MinorNumber testLess;
    MinorNumber testGreater;
    MinorNumber testEqual;

    public MinorNumberTest()
    {
    }

    @Before
    @SuppressWarnings("checkstyle:magicnumber")
    public void setUp()
        throws ValueOutOfRangeException
    {
        refNr = new MinorNumber(450);
        testLess = new MinorNumber(433);
        testGreater = new MinorNumber(477);
        testEqual = new MinorNumber(450);
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test MinorNumber object creation
     *
     * PERFORMANCE WARNING
     * There are normally ~1 million minor numbers, so all of them are tested.
     * If this ever changes to a huge value, the test will have to be limited to
     * avoid an extremely long execution time.
     *
     * @throws ValueOutOfRangeException
     */
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
                MinorNumber tmp = new MinorNumber(invalidNr);
                fail(String.format("Constructor succeeded with minor number = %d", invalidNr));
            }
            catch (ValueOutOfRangeException expectedExc)
            {
                // Construction failed as expected, nothing to do
            }
        }
        if (MinorNumber.MINOR_NR_MIN >= MinorNumber.MINOR_NR_MAX)
        {
            fail(String.format("Bad constants, MINOR_NR_MIN (%d) >= MINOR_NR_MAX (%d)",
                 MinorNumber.MINOR_NR_MIN, MinorNumber.MINOR_NR_MAX));
        }
        int validNr = MinorNumber.MINOR_NR_MIN;
        while (validNr <= MinorNumber.MINOR_NR_MAX)
        {
            MinorNumber tmp = new MinorNumber(validNr);
            if (tmp.value != validNr)
            {
                fail(String.format("MinorNumber value %d != construction minor number %d", tmp.value, validNr));
            }
            // Avoid overflow if MINOR_NR_MAX == Integer.MAX_VALUE
            if (validNr >= MinorNumber.MINOR_NR_MAX)
            {
                break;
            }
            ++validNr;
        }
    }

    /**
     * Test of compareTo method, of class MinorNumber.
     */
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

    /**
     * Test of equals method, of class MinorNumber.
     */
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

    /**
     * Test of hashCode method, of class MinorNumber.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testHashCode()
        throws ValueOutOfRangeException
    {
        // Hash code generation normally uses multiplication with prime numbers
        //
        // Use two different prime numbers to make it unlikely that those two
        // MinorNumbers generate the same hash.
        MinorNumber nrOne = new MinorNumber(111);
        MinorNumber nrTwo = new MinorNumber(251);

        // Create another MinorNumber object with the same value as the first one.
        // This should have the same hash value as the first one.
        MinorNumber nrThree = new MinorNumber(111);

        int hashOne = nrOne.hashCode();
        int hashTwo = nrTwo.hashCode();
        int hashThree = nrThree.hashCode();

        if (hashOne == hashTwo)
        {
            fail(String.format("hashCode() for minor numbers %d and %d is equal (%d).",
                 nrOne.value, nrTwo.value, hashOne));
        }
        if (hashOne != hashThree)
        {
            fail(String.format("hashCode() for two equal minor numbers (%d) differs (%d vs %d)",
                 nrOne.value, hashOne, hashThree));
        }
    }

    /**
     * Test of toString method, of class MinorNumber.
     */
    @Test
    public void testToString()
        throws ValueOutOfRangeException, NumberFormatException
    {
        for (int nr : TO_STRING_TESTS)
        {
            MinorNumber tmp = new MinorNumber(nr);
            String nrStr = tmp.toString();
            int checkNr = Integer.parseInt(nrStr);
            if (checkNr != nr)
            {
                fail(
                    String.format(
                        "MinorNumber toString() generated string \"%s\", which parsed to value %d instead of %d",
                        nrStr, checkNr, nr
                    )
                );
            }
        }
    }
}
