package com.linbit.linstor;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.types.TcpPortNumber;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpPortNumberTest
{
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int[] CTOR_FAIL_TESTS =
    {
        -1,
        0,
        Integer.MIN_VALUE,
        (1 << 16),
        Integer.MAX_VALUE
    };

    public static final int[] TO_STRING_TESTS =
    {
        1,
        750,
        65130
    };

    TcpPortNumber refNr;
    TcpPortNumber testLess;
    TcpPortNumber testGreater;
    TcpPortNumber testEqual;

    @SuppressWarnings("checkstyle:magicnumber")
    public TcpPortNumberTest()
        throws ValueOutOfRangeException
    {
        refNr = new TcpPortNumber(6799);
        testLess = new TcpPortNumber(230);
        testGreater = new TcpPortNumber(9811);
        testEqual = new TcpPortNumber(6799);
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
                TcpPortNumber tmp = new TcpPortNumber(invalidNr);
                fail(String.format("Constructor succeeded with port number = %d", invalidNr));
            }
            catch (ValueOutOfRangeException expectedExc)
            {
                // Construction failed as expected, nothing to do
            }
        }
        if (TcpPortNumber.PORT_NR_MIN >= TcpPortNumber.PORT_NR_MAX)
        {
            fail(String.format("Bad constants, PORT_NR_MIN (%d) >= PORT_NR_MAX (%d)",
                 TcpPortNumber.PORT_NR_MIN, TcpPortNumber.PORT_NR_MAX));
        }
        int validNr = TcpPortNumber.PORT_NR_MIN;
        while (validNr <= TcpPortNumber.PORT_NR_MAX)
        {
            TcpPortNumber tmp = new TcpPortNumber(validNr);
            if (tmp.value != validNr)
            {
                fail(String.format("TcpPortNumber value %d != construction port number %d", tmp.value, validNr));
            }
            // Avoid overflow if MINOR_NR_MAX == Integer.MAX_VALUE
            if (validNr >= TcpPortNumber.PORT_NR_MAX)
            {
                break;
            }
            ++validNr;
        }
    }

    /**
     * Test of compareTo method, of class TcpPortNumber.
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
     * Test of equals method, of class TcpPortNumber.
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
     * Test of hashCode method, of class TcpPortNumber.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testHashCode()
        throws ValueOutOfRangeException
    {
        // Hash code generation normally uses multiplication with prime numbers
        TcpPortNumber nrOne = new TcpPortNumber(37750);
        TcpPortNumber nrTwo = new TcpPortNumber(21);

        // Create another TcpPortNumber object with the same value as the first one.
        // This should have the same hash value as the first one.
        TcpPortNumber nrThree = new TcpPortNumber(37750);

        int hashOne = nrOne.hashCode();
        int hashTwo = nrTwo.hashCode();
        int hashThree = nrThree.hashCode();

        if (hashOne == hashTwo)
        {
            fail(String.format("hashCode() for port numbers %d and %d is equal (%d).",
                 nrOne.value, nrTwo.value, hashOne));
        }
        if (hashOne != hashThree)
        {
            fail(String.format("hashCode() for two equal port numbers (%d) differs (%d vs %d)",
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
            TcpPortNumber tmp = new TcpPortNumber(nr);
            String nrStr = tmp.toString();
            int checkNr = Integer.parseInt(nrStr);
            if (checkNr != nr)
            {
                fail(
                    String.format(
                        "TcpPortNumber toString() generated string \"%s\", which parsed to value %d instead of %d",
                        nrStr, checkNr, nr
                    )
                );
            }
        }
    }
}
