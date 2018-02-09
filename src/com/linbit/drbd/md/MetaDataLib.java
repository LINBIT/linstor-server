package com.linbit.drbd.md;

/**
 * Calculates DRBD data / meta data size using a native external library
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MetaDataLib extends MdCommon implements MetaDataApi
{
    public static final String NATIVE_LIBRARY_NAME = "DrbdMdCalc";

    // Loads the external library upon class loading
    static
    {
        System.loadLibrary(NATIVE_LIBRARY_NAME);
    }

    // Pre-loaded OutOfMemoryError instance required by the JNI functions
    public final OutOfMemoryError oomError = new OutOfMemoryError();


    @Override
    public long getNetSize(long grossSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(grossSize, peers, alStripes, alStripeSize);

        return libGetNetSize(grossSize, peers, alStripes, alStripeSize);
    }


    @Override
    public long getGrossSize(long netSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(netSize, peers, alStripes, alStripeSize);

        return libGetGrossSize(netSize, peers, alStripes, alStripeSize);
    }


    @Override
    public long getInternalMdSize(
        SizeSpec mode,
        long     size,
        short    peers,
        int      alStripes,
        long     alStripeSize
    )
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(size, peers, alStripes, alStripeSize);

        return libGetInternalMdSize(mode, size, peers, alStripes, alStripeSize);
    }


    @Override
    public long getExternalMdSize(long size, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(size, peers, alStripes, alStripeSize);

        return libGetExternalMdSize(size, peers, alStripes, alStripeSize);
    }


    private native long libGetNetSize(long grossSize, short peers, int alStripes, long alStripeSize)
        throws MinSizeException, MaxSizeException, MinAlSizeException, MaxAlSizeException,
               AlStripesException, PeerCountException;

    private native long libGetGrossSize(long netSize, short peers, int alStripes, long alStripeSize)
        throws MinSizeException, MaxSizeException, MinAlSizeException, MaxAlSizeException,
               AlStripesException, PeerCountException;

    private native long libGetInternalMdSize(
        SizeSpec mode,
        long     size,
        short    peers,
        int      alStripes,
        long     alStripeSize
    )
        throws MinSizeException, MaxSizeException, MinAlSizeException, MaxAlSizeException,
               AlStripesException, PeerCountException;

    private native long libGetExternalMdSize(long size, short peers, int alStripes, long alStripeSize)
        throws MinSizeException, MaxSizeException, MinAlSizeException, MaxAlSizeException,
               AlStripesException, PeerCountException;
}
