package com.linbit.drbd.md;

/**
 * Sanity checks for device size, peer count, activity log size and stripes count
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MdCommon
{
    protected void checkValid(long size, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException
    {
        if (size < 0 || peers < 0 || alStripes < 0 || alStripeSize < 0 ||
            peers > 0xFF || alStripes > 0xFFFF || alStripeSize > 0xFFFFFFFFl)
        {
            throw new IllegalArgumentException();
        }
    }
}
