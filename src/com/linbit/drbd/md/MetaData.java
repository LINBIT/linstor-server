package com.linbit.drbd.md;

import com.linbit.drbd.md.MetaDataApi.SizeSpec;

/**
 * Calculates DRBD data / meta data size
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MetaData extends MdCommon implements MetaDataApi
{
    // Alignment of the metadata area
    public static final int DRBD_MD_ALIGN_kiB = 4;

    // Alignment (or granularity) of the bitmap area
    public static final int DRBD_BM_ALIGN_kiB = 4;

    // Alignment (or granularity) in bytes of the bitmap for a single peer.
    // The bitmap size is increased or decreased in steps of DRBD_BM_PEER_ALIGN bytes.
    public static final int DRBD_BM_PEER_ALIGN = 8;

    // Data size in kiB covered by one bitmap bit
    public static final int DRBD_BM_BIT_COVER_kiB = 4;

    // Data size covered by one bitmap byte
    public static final int DRBD_BM_BYTE_COVER_kiB = 32;

    // Default size of the activity log
    public static final int DRBD_DEFAULT_AL_kiB = 32;

    // Minimum size of the activity log
    public static final int DRBD_MIN_AL_kiB = 4;

    // Maximum size of the activity log
    public static final int DRBD_MAX_AL_kiB = 1048576;

    // Alignment of the activity log
    public static final int DRBD_AL_ALIGN_kiB = 4;

    // Size of the DRBD meta data superblock
    public static final int DRBD_MD_SUPERBLK_kiB = 4;

    // Maximum size in kiB of a DRBD-replicated device
    // Must be a multiple of DRBD_BM_BIT_COVER_kiB
    public static final long DRBD_MAX_kiB = 1L << 40;

    // Minimum gross size (including metadata) of a DRBD-replicated device
    public static final long DRBD_MIN_GROSS_kiB = 68;

    // Minimum net size (without metadata) of a DRBD-replicated device
    public static final long DRBD_MIN_NET_kiB = 4;

    // Minimum number of peers
    public static final short DRBD_MIN_PEERS = 1;

    // Maximum number of peers
    public static final short DRBD_MAX_PEERS = 31;


    @Override
    public long getNetSize(long grossSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(grossSize, peers, alStripes, alStripeSize);

        long bitmapSize = getBitmapInternalSizeGross(grossSize, peers);
        long alSize = getAlSize(alStripes, alStripeSize);
        long mdSize = alignUp(bitmapSize + alSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
        grossSize = alignDown(grossSize, DRBD_BM_BIT_COVER_kiB);

        if (mdSize >= grossSize)
        {
            throw new MinSizeException();
        }

        long netSize = grossSize - mdSize;

        return netSize;
    }


    @Override
    public long getGrossSize(long netSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(netSize, peers, alStripes, alStripeSize);

        long bitmapSize = getBitmapInternalSizeNet(netSize, peers, alStripes, alStripeSize);
        long alSize = getAlSize(alStripes, alStripeSize);
        long mdSize = alignUp(bitmapSize + alSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
        long grossSize = alignUp(netSize, DRBD_BM_BIT_COVER_kiB) + mdSize;

        checkMaxDrbdSize(grossSize);

        if (grossSize < DRBD_MIN_GROSS_kiB)
        {
            grossSize = DRBD_MIN_GROSS_kiB;
        }

        return grossSize;
    }


    @Override
    public long getInternalMdSize(
        SizeSpec mode,
        long    size,
        short   peers,
        int     alStripes,
        long    alStripeSize
    )
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(size, peers, alStripes, alStripeSize);

        size = alignUp(size, DRBD_BM_BIT_COVER_kiB);

        long mdSize = 0;
        long alSize = getAlSize(alStripes, alStripeSize);
        switch (mode)
        {
            case netSize:
            {
                long bitmapSize = getBitmapInternalSizeNet(size, peers, alStripes, alStripeSize);
                mdSize = alignUp(alSize + bitmapSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
                break;
            }
            case grossSize:
            {
                long bitmapSize = getBitmapInternalSizeGross(size, peers);
                mdSize = alignUp(alSize + bitmapSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
                break;
            }
            default:
                throw new IllegalArgumentException();
        }

        return mdSize;
    }


    @Override
    public long getExternalMdSize(long size, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException
    {
        checkValid(size, peers, alStripes, alStripeSize);

        checkMaxDrbdSize(size);
        checkPeers(peers);

        size = alignUp(size, DRBD_BM_BIT_COVER_kiB);

        long alSize = getAlSize(alStripes, alStripeSize);
        long bitmapSize = getBitmapExternalSize(size, peers);
        long mdSize = alignUp(alSize + bitmapSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);

        return mdSize;
    }


    private long ceilingDivide(long dividend, long divisor)
    {
        long quotient = dividend / divisor;
        if (dividend % divisor != 0)
        {
            ++quotient;
        }
        return quotient;
    }


    private long alignUp(long value, long alignment)
    {
        if (value % alignment != 0)
        {
            value = ((value / alignment) + 1) * alignment;
        }
        return value;
    }


    private long alignDown(long value, long alignment)
    {
        return (value / alignment) * alignment;
    }


    private long getAlSize(
        int  alStripes,
        long alStripeSize
    )
        throws AlStripesException, MaxAlSizeException, MinAlSizeException
    {
        if (alStripes < 1)
        {
            throw new AlStripesException();
        }

        if (alStripeSize > DRBD_MAX_AL_kiB)
        {
            throw new MaxAlSizeException();
        }

        long alSize = alStripeSize * alStripes;
        alSize = alignUp(alSize, DRBD_AL_ALIGN_kiB);

        if (alSize < DRBD_MIN_AL_kiB)
        {
            throw new MinAlSizeException();
        }

        if (alSize > DRBD_MAX_AL_kiB)
        {
            throw new MaxAlSizeException();
        }

        return alSize;
    }


    public void checkPeers(short peers) throws PeerCountException
    {
        if (peers < DRBD_MIN_PEERS || peers > DRBD_MAX_PEERS)
        {
            throw new PeerCountException();
        }
    }


    public void checkMinDrbdSizeNet(long netSize) throws MinSizeException
    {
        if (netSize < DRBD_MIN_NET_kiB)
        {
            throw new MinSizeException();
        }
    }


    public void checkMinDrbdSizeGross(long grossSize) throws MinSizeException
    {
        if (grossSize < DRBD_MIN_GROSS_kiB)
        {
            throw new MinSizeException();
        }
    }


    public void checkMaxDrbdSize(long size) throws MaxSizeException
    {
        if (size > DRBD_MAX_kiB)
        {
            throw new MaxSizeException();
        }
    }


    private long getBitmapExternalSize(long size, short peers)
        throws MinSizeException, MaxSizeException, PeerCountException
    {
        checkMinDrbdSizeNet(size);
        checkMaxDrbdSize(size);
        checkPeers(peers);

        size = alignUp(size, DRBD_BM_BIT_COVER_kiB);

        long bitmapPeerBytes = ceilingDivide(size, DRBD_BM_BYTE_COVER_kiB);
        bitmapPeerBytes = alignUp(bitmapPeerBytes, DRBD_BM_PEER_ALIGN);
        long bitmapBytes = bitmapPeerBytes * peers;
        long bitmapSize = alignUp(alignUp(bitmapBytes, 1024) / 1024, DRBD_BM_ALIGN_kiB);

        return bitmapSize;
    }


    private long getBitmapInternalSizeNet(
        long  netSize,
        short peers,
        int   alStripes,
        long  alStripeSize
    )
        throws MinSizeException, MaxSizeException, PeerCountException,
               AlStripesException, MaxAlSizeException, MinAlSizeException
    {
        checkMinDrbdSizeNet(netSize);
        checkMaxDrbdSize(netSize);
        checkPeers(peers);

        netSize = alignUp(netSize, DRBD_BM_BIT_COVER_kiB);

        long alSize = getAlSize(alStripes, alStripeSize);

        // Base size for the recalculation of the gross size
        // in each iteration of the sequence limit loop
        // The base size is the net data + activity log + superblock,
        // but without the bitmap
        long baseSize = netSize + alSize + DRBD_MD_SUPERBLK_kiB;

        // Calculate the size of the bitmap required to cover the
        // gross size of the device, which includes the size of the bitmap.
        // The factor for determining the bitmap size required to cover the gross size of a DRBD replicated
        // device (which is the net size + bitmap size + activity log size + superblock size, and potentially
        // various deviations from each value due to alignment requirements) is the limit of the sum
        // of a geometric series. The function is guaranteed to be convergent.
        // To avoid doing complex float-calculations, alignment-corrections and conversions from integer
        // to float types and back again, the limit function is instead run as a loop that calculates a
        // more precise bitmap size with every iteration, until the precision is sufficient
        // for the size of data to be covered by the bitmap, and the bitmap is sized sufficiently large
        // to cover the gross data size, including the bitmap itself.
        long grossSize = baseSize;
        long bitmapSize = 0;
        long bitmapCoverSize = 0;
        while (bitmapCoverSize < grossSize)
        {
            // Bitmap size required to cover the gross size on each peer
            long bitmapPeerBytes = ceilingDivide(grossSize, DRBD_BM_BYTE_COVER_kiB);
            // Align to the per-peer bitmap granularity
            bitmapPeerBytes = alignUp(bitmapPeerBytes, DRBD_BM_PEER_ALIGN);

            // Bitmap size for all peers
            long bitmapBytes = bitmapPeerBytes * peers;

            // Gross size covered by the current bitmap size
            bitmapCoverSize = bitmapPeerBytes * DRBD_BM_BYTE_COVER_kiB;

            // Actual size of the bitmap in the DRBD metadata area (after alignment)
            bitmapSize = alignUp(alignUp(bitmapBytes, 1024) / 1024, DRBD_BM_ALIGN_kiB);
            // Resulting gross size after including the bitmap size
            grossSize = baseSize + bitmapSize;
        }

        checkMaxDrbdSize(grossSize);

        return bitmapSize;
    }


    private long getBitmapInternalSizeGross(long  grossSize, short peers)
        throws MinSizeException, MaxSizeException, PeerCountException
    {
        checkMinDrbdSizeGross(grossSize);
        checkMaxDrbdSize(grossSize);
        checkPeers(peers);

        grossSize = alignUp(grossSize, DRBD_BM_BIT_COVER_kiB);

        long bitmapPeerBytes = ceilingDivide(grossSize, DRBD_BM_BYTE_COVER_kiB);
        bitmapPeerBytes = alignUp(bitmapPeerBytes, DRBD_BM_PEER_ALIGN);
        long bitmapBytes = bitmapPeerBytes * peers;
        long bitmapSize = alignUp(alignUp(bitmapBytes, 1024) / 1024, DRBD_BM_ALIGN_kiB);

        return bitmapSize;
    }
}
