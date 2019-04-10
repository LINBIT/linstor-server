package com.linbit.linstor;

import com.linbit.StringConv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the various string conversions in the StringConv class
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class StringConvTest
{

    public StringConvTest()
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

    /**
     * Test of getDfltBoolean method, of class StringConv.
     */
    @Test
    public void testGetDfltBoolean() throws Exception
    {

        assertTrue(
            "Failed to parse Boolean.toString(true) value",
            StringConv.getDfltBoolean(Boolean.toString(true), false)
        );

        assertFalse(
            "Failed to parse Boolean.toString(false) value",
            StringConv.getDfltBoolean(Boolean.toString(false), true)
        );

        assertTrue(
            "Failed to parse \"TRUE\"",
            StringConv.getDfltBoolean("TRUE", false)
        );

        assertFalse(
            "Failed to parse \"FALSE\"",
            StringConv.getDfltBoolean("FALSE", true)
        );

        assertTrue(
            "Failed to parse \"true\"",
            StringConv.getDfltBoolean("true", false)
        );

        assertFalse(
            "Failed to parse \"false\"",
            StringConv.getDfltBoolean("false", true)
        );

        assertTrue(
            "Failed to parse \"True\"",
            StringConv.getDfltBoolean("True", false)
        );

        assertFalse(
            "Failed to parse \"False\"",
            StringConv.getDfltBoolean("False", true)
        );

        assertTrue(
            "Failed to parse \"tRuE\"",
            StringConv.getDfltBoolean("tRuE", false)
        );

        assertFalse(
            "Failed to parse \"FaLSe\"",
            StringConv.getDfltBoolean("FaLSe", true)
        );

        assertTrue(
            "Failed to return true result for null input value",
            StringConv.getDfltBoolean(null, true)
        );

        assertFalse(
            "Failed to return false result for null input value",
            StringConv.getDfltBoolean(null, false)
        );

        assertTrue(
            "Failed to return true result for invalid input value",
            StringConv.getDfltBoolean("There is an orange rubber duck in the treasure chest!!", true)
        );

        assertFalse(
            "Failed to return false result for invalid input value",
            StringConv.getDfltBoolean("There is an orange rubber duck in the treasure chest!!", false)
        );
    }

    /**
     * Test of getDfltByte method, of class StringConv.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testGetDfltByte() throws Exception
    {
        if (StringConv.getDfltByte(Byte.toString((byte) 42), (byte) 0) != (byte) 42)
        {
            fail("Failed to parse Byte.toString((byte) 42)");
        }

        if (StringConv.getDfltByte(Byte.toString((byte) -99), (byte) 0) != (byte) -99)
        {
            fail("Failed to parse Byte.toString((byte) -99)");
        }

        if (StringConv.getDfltByte("+-41", (byte) 123) != (byte) 123)
        {
            fail("Failed to return default value for invalid input \"+-41\"");
        }

        if (StringConv.getDfltByte("-431", (byte) 65) != (byte) 65)
        {
            fail("Failed to return default value for invalid input \"-431\"");
        }

        if (StringConv.getDfltByte("", (byte) -76) != (byte) -76)
        {
            fail("Failed to return default value for empty input");
        }

        if (StringConv.getDfltByte(null, (byte) 0) != (byte) 0)
        {
            fail("Failed to return default value for null input value");
        }
    }

    /**
     * Test of getDfltShort method, of class StringConv.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testGetDfltShort() throws Exception
    {
        if (StringConv.getDfltShort(Short.toString((short) 31337), (short) 0) != (short) 31337)
        {
            fail("Failed to parse Short.toString((short) 31337)");
        }

        if (StringConv.getDfltShort(Short.toString((short) -1337), (short) 0) != (short) -1337)
        {
            fail("Failed to parse Short.toString((short) -1337)");
        }

        if (StringConv.getDfltShort("blubb", (short) 137) != (short) 137)
        {
            fail("Failed to return default value for invalid input \"blubb\"");
        }

        if (StringConv.getDfltShort("69111", (short) 21) != (short) 21)
        {
            fail("Failed to return default value for invalid input \"69111\"");
        }

        if (StringConv.getDfltShort("", (short) 101) != (short) 101)
        {
            fail("Failed to return default value for empty input");
        }

        if (StringConv.getDfltShort(null, (short) 101) != (short) 101)
        {
            fail("Failed to return default value for null input value");
        }
    }

    /**
     * Test of getDfltInt method, of class StringConv.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testGetDfltInt() throws Exception
    {
        if (StringConv.getDfltInt(Integer.toString(19191), 0) != 19191)
        {
            fail("Failed to parse Integer.toString(19191)");
        }

        if (StringConv.getDfltInt(Integer.toString(-72727), 0) != -72727)
        {
            fail("Failed to parse Integer.toString(-72727)");
        }

        if (StringConv.getDfltInt(" ++1", Integer.MIN_VALUE) != Integer.MIN_VALUE)
        {
            fail("Failed to return default value for invalid input \" ++1\"");
        }

        if (StringConv.getDfltInt("onehundredfortyseven", -1) != -1)
        {
            fail("Failed to return default value for invalid input \"onehundredfortyseven\"");
        }

        if (StringConv.getDfltInt("", 1870645533) != 1870645533)
        {
            fail("Failed to return default value for empty input");
        }

        if (StringConv.getDfltInt(null, 931777212) != 931777212)
        {
            fail("Failed to return default value for null input value");
        }
    }

    /**
     * Test of getDfltLong method, of class StringConv.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testGetDfltLong() throws Exception
    {
        if (StringConv.getDfltLong(Long.toString(100), 0L) != 100L)
        {
            fail("Failed to parse Long.toString(100L)");
        }

        if (StringConv.getDfltLong(Long.toString(-1), 0L) != -1L)
        {
            fail("Failed to parse Long.toString(-1L)");
        }

        if (StringConv.getDfltLong("--1", 0L) != 0L)
        {
            fail("Failed to return default value for invalid input \"--1\"");
        }

        if (StringConv.getDfltLong("three", -70L) != -70L)
        {
            fail("Failed to return default value for invalid input \"three\"");
        }

        if (StringConv.getDfltLong("", 7235728385971823L) != 7235728385971823L)
        {
            fail("Failed to return default value for empty input");
        }

        if (StringConv.getDfltLong(null, -562938497857234L) != -562938497857234L)
        {
            fail("Failed to return default value for null input value");
        }
    }

    /**
     * Test of getDfltFloat method, of class StringConv.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testGetDfltFloat() throws Exception
    {
        if (StringConv.getDfltFloat(Float.toString((float) 37.1), 0) != (float) 37.1)
        {
            fail("Failed to parse Float.toString((float) 37.1)");
        }

        if (StringConv.getDfltFloat(Float.toString((float) -1.37), 0) != (float) -1.37)
        {
            fail("Failed to parse Float.toString((float) -1.37)");
        }

        if (StringConv.getDfltFloat("--1", 2) != 2)
        {
            fail("Failed to return default value for invalid input \"--1\"");
        }

        if (StringConv.getDfltFloat("three", (float) -1455.5) != (float) -1455.5)
        {
            fail("Failed to return default value for invalid input \"three\"");
        }

        if (StringConv.getDfltFloat("", 0) != 0)
        {
            fail("Failed to return default value for empty input");
        }

        if (StringConv.getDfltFloat(null, (float) 0.1) != (float) 0.1)
        {
            fail("Failed to return default value for null input value");
        }
    }

    /**
     * Test of getDfltDouble method, of class StringConv.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testGetDfltDouble() throws Exception
    {
        if (StringConv.getDfltDouble(Double.toString(43.3), Double.MIN_NORMAL) != 43.3)
        {
            fail("Failed to parse Double.toString(43.3)");
        }

        if (StringConv.getDfltDouble(Double.toString(-65535.1), Double.MIN_NORMAL) != -65535.1)
        {
            fail("Failed to parse Double.toString(43.3)");
        }

        if (StringConv.getDfltDouble("Qapla'", -873.346) != -873.346)
        {
            fail("Failed to return default value for invalid input \"Qapla'\"");
        }

        if (StringConv.getDfltDouble("-+1 ", -65535.1) != -65535.1)
        {
            fail("Failed to return default value for invalid input \"-+1 \"");
        }

        if (StringConv.getDfltDouble("", 893) != 893)
        {
            fail("Failed to return default value for empty input");
        }

        if (StringConv.getDfltDouble(null, 713764.565) != 713764.565)
        {
            fail("Failed to return default value for null input value");
        }
    }
}
