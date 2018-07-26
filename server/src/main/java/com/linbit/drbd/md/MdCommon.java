package com.linbit.drbd.md;

/**
 * Sanity checks for device size, peer count, activity log size and stripes count
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MdCommon
{
    public static final short   MAX_PEERS           = 0xFF;
    public static final int     MAX_AL_STRIPES      = 0xFFFF;
    public static final long    MAX_AL_STRIPE_SIZE  = 0xFFFFFFFFl;

    protected void checkValid(final long size, final short peers, final int alStripes, final long alStripeSize)
        throws IllegalArgumentException
    {
        if (size < 0 || peers < 0 || alStripes < 0 || alStripeSize < 0 ||
            peers > MAX_PEERS || alStripes > MAX_AL_STRIPES || alStripeSize > MAX_AL_STRIPE_SIZE)
        {
            throw new IllegalArgumentException();
        }
    }
}
