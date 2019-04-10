package com.linbit.drbd.md;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test DRBD meta data size calculation
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@SuppressWarnings("checkstyle:magicnumber")
public class MetaDataApiTest
{
    private MetaDataApi md;

    public MetaDataApiTest()
    {
    }

    @Before
    public void setUp()
    {
        md = new MetaData();
    }

    @Test
    public void testNegativeAlStripes() throws Exception
    {
        int alStripes = -1;

        try
        {
            md.getNetSize(40960L, (short) 7, alStripes, 64);
            fail("getNetSize: allows alStripes == -1");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        try
        {
            md.getGrossSize(40960L, (short) 7, alStripes, 64);
            fail("getGrossSize: allows alStripes == -1");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(40960L, (short) 7, alStripes, 64);
            fail("getExternalMdSize: allows alStripes == -1");
        }
        catch (IllegalArgumentException ignored)
        {
        }
    }

    @Test
    public void testZeroAlStripes() throws Exception
    {
        int alStripes = 0;

        try
        {
            md.getNetSize(40960L, (short) 7, alStripes, 64);
            fail("getNetSize: allows alStripes == 0");
        }
        catch (AlStripesException ignored)
        {
        }

        try
        {
            md.getGrossSize(40960L, (short) 7, alStripes, 64);
            fail("getGrossSize: allows alStripes == 0");
        }
        catch (AlStripesException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(40960L, (short) 7, alStripes, 64);
            fail("getExternalMdSize: allows alStripes == 0");
        }
        catch (AlStripesException ignored)
        {
        }
    }

    @Test
    public void testAlTooBig() throws Exception
    {
        int alStripes = 0xFF;
        long alStripeSize = 0xFFFFFFFFL;

        try
        {
            md.getNetSize(40960L, (short) 7, alStripes, alStripeSize);
            fail("getNetSize: allows exceeding the maximum activity log size");
        }
        catch (AlStripesException | MaxAlSizeException ignored)
        {
        }

        try
        {
            md.getGrossSize(40960L, (short) 7, alStripes, alStripeSize);
            fail("getGrossSize: allows exceeding the maximum activity log size");
        }
        catch (AlStripesException | MaxAlSizeException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(40960L, (short) 7, alStripes, alStripeSize);
            fail("getExternalMdSize: allows exceeding the maximum activity log size");
        }
        catch (AlStripesException | MaxAlSizeException ignored)
        {
        }
    }

    @Test
    public void testZeroAlSizeCheck() throws Exception
    {
        int alStripes = 1;
        long alStripeSize = 0;

        try
        {
            md.getNetSize(40960L, (short) 7, alStripes, alStripeSize);
            fail("getNetSize: failed to throw MinAlSizeException");
        }
        catch (MinAlSizeException ignored)
        {
        }

        try
        {
            md.getGrossSize(40960L, (short) 7, alStripes, alStripeSize);
            fail("getGrossSize: allows exceeding the maximum activity log size");
        }
        catch (MinAlSizeException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(40960L, (short) 7, alStripes, alStripeSize);
            fail("getExternalMdSize: allows exceeding the maximum activity log size");
        }
        catch (MinAlSizeException ignored)
        {
        }
    }

    @Test
    public void testDataTooBig() throws Exception
    {
        try
        {
            md.getNetSize(1099511627777L, (short) 31, 1, 32);
            fail("getNetSize: failed to throw MaxSizeException");
        }
        catch (MaxSizeException ignored)
        {
        }

        try
        {
            md.getGrossSize(1098471440349L, (short) 31, 1, 32);
            fail("getGrossSize: failed to throw MaxSizeException");
        }
        catch (MaxSizeException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(1099511627777L, (short) 31, 1, 32);
            fail("getExternalMdSize: failed to throw MaxSizeException");
        }
        catch (MaxSizeException ignored)
        {
        }
    }

    @Test
    public void testDataTooSmall() throws Exception
    {
        try
        {
            md.getNetSize(67L, (short) 1, 1, 32);
            fail("getNetSize: failed to throw MinSizeException");
        }
        catch (MinSizeException ignored)
        {
        }

        // The net size calculations round up to 4 kiB boundaries,
        // therefore, this test does not apply to those functions
    }

    @Test
    public void testMaxPeerCount() throws Exception
    {
        short peers = 32;

        try
        {
            md.getNetSize(40960L, peers, 1, 64);
            fail("getNetSize: failed to throw PeerCountException");
        }
        catch (PeerCountException ignored)
        {
        }

        try
        {
            md.getGrossSize(40960L, peers, 1, 64);
            fail("getGrossSize: failed to throw PeerCountException");
        }
        catch (PeerCountException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(40960L, peers, 1, 64);
            fail("getExternalMdSize: failed to throw PeerCountException");
        }
        catch (PeerCountException ignored)
        {
        }
    }

    @Test
    public void testZeroPeerCount() throws Exception
    {
        short peers = 0;

        try
        {
            md.getNetSize(40960L, peers, 1, 64);
            fail("getNetSize: failed to throw PeerCountException");
        }
        catch (PeerCountException ignored)
        {
        }

        try
        {
            md.getGrossSize(40960L, peers, 1, 64);
            fail("getGrossSize: failed to throw PeerCountException");
        }
        catch (PeerCountException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(40960L, peers, 1, 64);
            fail("getExternalMdSize: failed to throw PeerCountException");
        }
        catch (PeerCountException ignored)
        {
        }
    }

    @Test
    public void testNegativePeerCount() throws Exception
    {
        short peers = -1;

        try
        {
            md.getNetSize(40960L, peers, 1, 64);
            fail("getNetSize: failed to throw IllegalArgumentException");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        try
        {
            md.getGrossSize(40960L, peers, 1, 64);
            fail("getGrossSize: failed to throw IllegalArgumentException");
        }
        catch (IllegalArgumentException ignored)
        {
        }

        try
        {
            md.getExternalMdSize(40960L, peers, 1, 64);
            fail("getExternalMdSize: failed to throw IllegalArgumentException");
        }
        catch (IllegalArgumentException ignored)
        {
        }
    }

    /**
     * Test of getNetSize method, of class MetaDataApi.
     */
    @Test
    public void testGetNetSize() throws Exception
    {
        long grossSize = 2048L;
        short peers = 5;
        int alStripes = 1;
        long alStripeSize = 32L;

        long expResult = 2008L;
        long result = md.getNetSize(grossSize, peers, alStripes, alStripeSize);
        assertEquals(expResult, result);
    }

    /**
     * Test of getGrossSize method, of class MetaDataApi.
     */
    @Test
    public void testGetGrossSize() throws Exception
    {
        long netSize = 2048L;
        short peers = 15;
        int alStripes = 2;
        long alStripeSize = 64L;

        long expResult = 2184L;

        long result = md.getGrossSize(netSize, peers, alStripes, alStripeSize);
        assertEquals(expResult, result);
    }

    /**
     * Test of getInternalMdSize method, of class MetaDataApi.
     */
    @Test
    public void testGetInternalMdSize() throws Exception
    {
        MetaDataApi.SizeSpec mode = MetaDataApi.SizeSpec.NET_SIZE;
        long size = 245603100L;
        short peers = 11;
        int alStripes = 4;
        long alStripeSize = 2048L;

        long expResult = 90676L;
        long result = md.getInternalMdSize(mode, size, peers, alStripes, alStripeSize);
        assertEquals(expResult, result);
    }

    /**
     * Test of getExternalMdSize method, of class MetaDataApi.
     */
    @Test
    public void testGetExternalMdSize() throws Exception
    {
        long size = 470000000L;
        short peers = 9;
        int alStripes = 2;
        long alStripeSize = 4096L;

        long expResult = 137288L;
        long result = md.getExternalMdSize(size, peers, alStripes, alStripeSize);
        assertEquals(expResult, result);
    }
}
