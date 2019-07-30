package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.Checks;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.types.LsIpAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the Checks class methods
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ChecksTest
{
    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-' };
    public static final int TEST_MIN_LENGTH = 2;
    public static final int TEST_MAX_LENGTH = 15;

    public ChecksTest()
    {
    }

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
    }

    @Test
    public void testValidNameShort() throws Exception
    {
        Checks.nameCheck("ab", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test
    public void testValidNameLong() throws Exception
    {
        Checks.nameCheck("veryLong_Name-1", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test
    public void testValidFirstCharName() throws Exception
    {
        Checks.nameCheck("_a", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test
    public void testValidInnerCharName() throws Exception
    {
        Checks.nameCheck("_v-", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test(expected = InvalidNameException.class)
    public void testInvalidNoAlphaName() throws Exception
    {
        Checks.nameCheck("_-0-3-1", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test(expected = InvalidNameException.class)
    public void testInvalidNumberFirstCharName() throws Exception
    {
        Checks.nameCheck("0test", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test(expected = InvalidNameException.class)
    public void testTooLongName() throws Exception
    {
        Checks.nameCheck("_tooLongName-013", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test(expected = InvalidNameException.class)
    public void testTooShortName() throws Exception
    {
        Checks.nameCheck("z", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test(expected = InvalidNameException.class)
    public void testInvalidInnerCharName() throws Exception
    {
        Checks.nameCheck("Black:Bear", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test(expected = InvalidNameException.class)
    public void testInvalidFirstCharName() throws Exception
    {
        Checks.nameCheck("-goNuts", TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test(expected = ImplementationError.class)
    public void testNullName() throws Exception
    {
        Checks.nameCheck(null, TEST_MIN_LENGTH, TEST_MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    @Test
    public void testValidHostname() throws Exception
    {
        Checks.hostNameCheck("martini-0.linbit");
    }

    @Test(expected = InvalidNameException.class)
    public void testTooShortHostname() throws Exception
    {
        Checks.hostNameCheck("m");
    }

    @Test(expected = InvalidNameException.class)
    public void testInvalidFirstCharHostname() throws Exception
    {
        Checks.hostNameCheck(".martini-0.linbit");
    }

    @Test(expected = InvalidNameException.class)
    public void testInvalidLastCharHostname() throws Exception
    {
        Checks.hostNameCheck("martini-0.linbit-");
    }

    @Test(expected = InvalidNameException.class)
    public void testInvalidCharHostname() throws Exception
    {
        Checks.hostNameCheck("martini:0.linbit");
    }

    @Test(expected = InvalidNameException.class)
    public void testTooLongLabelHostname() throws Exception
    {
        Checks.hostNameCheck(
            "martini-0.ridiculouslyLongSubDomainName-" +
            "1234567890123456789012345678901234.linbit"
        );
    }

    @Test(expected = InvalidNameException.class)
    public void testTooLongHostname() throws Exception
    {
        Checks.hostNameCheck(
            "A123456789012345678901234567890123456789" +
            ".123456789012345678901234567890123456789" +
            ".123456789012345678901234567890123456789" +
            ".123456789012345678901234567890123456789" +
            ".123456789012345678901234567890123456789" +
            ".123456789012345678901234567890123456789" +
            "0123456789abcdef"
        );
    }

    @Test(expected = ImplementationError.class)
    public void testNullHostname() throws Exception
    {
        Checks.hostNameCheck(null);
    }

    @Test
    public void testWithinRange() throws Exception
    {
        Checks.rangeCheck(0, -1, 1);
    }

    @Test(expected = ValueOutOfRangeException.class)
    public void testTooHigh() throws Exception
    {
        Checks.rangeCheck(2, Long.MIN_VALUE, 1);
    }

    @Test(expected = ValueOutOfRangeException.class)
    public void testTooLow() throws Exception
    {
        Checks.rangeCheck(1, 2, Long.MAX_VALUE);
    }

    @Test(expected = ValueOutOfRangeException.class)
    public void testNegativeTooLow() throws Exception
    {
        Checks.rangeCheck(-2, -1, 1);
    }

    @Test(expected = ValueOutOfRangeException.class)
    public void testNegativeTooHigh() throws Exception
    {
        Checks.rangeCheck(-1, Long.MIN_VALUE, -2);
    }

    @Test(expected = ImplementationError.class)
    public void testImpossibleRange() throws Exception
    {
        // minValue > maxValue
        Checks.rangeCheck(0, 1, -1);
    }

    @Test
    public void testIpV4() throws Exception
    {
        new LsIpAddress("127.0.0.1");
        new LsIpAddress("0.0.0.0");
        new LsIpAddress("255.255.255.255");
    }

    @Test(expected = InvalidIpAddressException.class)
    public void testIpV4Invalid1() throws Exception
    {
        new LsIpAddress("0.0.0.0.0");
    }

    @Test(expected = InvalidIpAddressException.class)
    public void testIpV4Invalid2() throws Exception
    {
        new LsIpAddress("0.0.0.a");
    }

    @Test(expected = InvalidIpAddressException.class)
    public void testIpV4Invalid3() throws Exception
    {
        new LsIpAddress("256.0.0.0");
    }

    @Test
    public void testIpV6() throws Exception
    {
        new LsIpAddress("::0");
        new LsIpAddress("2001:0db8:0000:0042:0000:8a2e:0370:7334");
        new LsIpAddress("0000:0000:0000:0000:0000:0000:0000:0000");
        new LsIpAddress("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff");
        new LsIpAddress("FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF");
        new LsIpAddress("2001::4930:203f");
        new LsIpAddress("2001::");

        new LsIpAddress("2001:db8:aaaa:bbbb:cccc:dddd::1");
        new LsIpAddress("2001:db8:aaaa:bbbb:cccc:dddd:0:1");

        new LsIpAddress("2001:db8:0:0:0::1");
        new LsIpAddress("2001:db8:0:0::1");
        new LsIpAddress("2001:db8:0::1");
        new LsIpAddress("2001:db8::1");
    }

    @Test
    public void testIpV6Mapped4() throws Exception
    {
        new LsIpAddress("ABCD:ABCD:ABCD:ABCD:ABCD:ABCD:192.168.158.190");
    }

    @Test(expected = InvalidIpAddressException.class)
    public void testIpV6Invalid1() throws Exception
    {
        new LsIpAddress("abcde:0000:0000::0");
    }

    @Test(expected = InvalidIpAddressException.class)
    public void testIpV6Invalid2() throws Exception
    {
        new LsIpAddress("1234::4312::0");
    }

    @Test(expected = InvalidIpAddressException.class)
    public void testIpV6Invalid3() throws Exception
    {
        new LsIpAddress("1234::4312:");
    }
}
