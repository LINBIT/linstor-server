package com.linbit.drbd.md;

import javax.inject.Inject;
import com.linbit.drbd.md.MetaDataApi.SizeSpec;

/**
 * Calculates DRBD data / meta data size
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MetaData extends MdCommon implements MetaDataApi
{
    // Alignment of the data device
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_DATA_ALIGN_kiB = 4;

    // Alignment of the metadata area
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_MD_ALIGN_kiB = 4;

    // Alignment (or granularity) of the bitmap area
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_BM_ALIGN_kiB = 4;

    // Alignment (or granularity) in bytes of the bitmap for a single peer.
    // The bitmap size is increased or decreased in steps of DRBD_BM_PEER_ALIGN bytes.
    public static final int DRBD_BM_PEER_ALIGN = 8;

    // Minimum data size in kiB covered by one bitmap bit
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_MIN_BM_BIT_COVER_kiB = 4;

    // Maximum data size in kiB covered by one bitmap bit
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_MAX_BM_BIT_COVER_kiB = 1024;

    // Default data size in kiB covered by one bitmap bit
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_DEFAULT_BM_BIT_COVER_kiB = 4;

    // Default size of the activity log
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_DEFAULT_AL_kiB = 32;

    // Minimum size of the activity log
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_MIN_AL_kiB = 4;

    // Maximum size of the activity log
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_MAX_AL_kiB = 1048576;

    // Alignment of the activity log
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_AL_ALIGN_kiB = 4;

    // Size of the DRBD meta data superblock
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final int DRBD_MD_SUPERBLK_kiB = 4;

    // Maximum size in kiB of a DRBD-replicated device
    // Must be a multiple of DRBD_MAX_BM_BIT_COVER_kiB
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final long DRBD_MAX_kiB = 1L << 40;

    // Minimum gross size (including metadata) of a DRBD-replicated device
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final long DRBD_MIN_GROSS_kiB = 68;

    // Minimum net size (without metadata) of a DRBD-replicated device
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final long DRBD_MIN_NET_kiB = 4;

    // Minimum number of peers
    public static final short DRBD_MIN_PEERS = 1;

    // Maximum number of peers
    public static final short DRBD_MAX_PEERS = 31;

    // Divisor for kiB alignment
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    public static final long DIVISOR_kiB = 1024;

    // Minimum size of external meta data
    // Naming convention exception: SI unit capitalization rules
    @SuppressWarnings("checkstyle:constantname")
    private static final long DRBD_MIN_EXT_MD_kiB = 1024;

    @Inject
    public MetaData()
    {
    }

    @Override
    public long getNetSize(
        final long      grossSize,
        final short     peers,
        final int       alStripes,
        final long      alStripeSize,
        final int       bitmapBlockSize
    )
        throws MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException
    {
        checkValid(grossSize, peers, alStripes, alStripeSize, bitmapBlockSize);
        checkMaxDrbdSize(grossSize);

        final long bitmapSize = getBitmapInternalSizeGross(grossSize, peers, bitmapBlockSize);
        final long alSize = getAlSize(alStripes, alStripeSize);
        final long mdSize = ceilingAlign(bitmapSize + alSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
        final long alignedGrossSize = floorAlign(grossSize, DRBD_DATA_ALIGN_kiB);

        if (mdSize >= alignedGrossSize)
        {
            throw new MinSizeException();
        }

        final long netSize = alignedGrossSize - mdSize;

        return netSize;
    }


    @Override
    public long getGrossSize(
        final long      netSize,
        final short     peers,
        final int       alStripes,
        final long      alStripeSize,
        final int       bitmapBlockSize
    )
        throws MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException
    {
        checkValid(netSize, peers, alStripes, alStripeSize, bitmapBlockSize);

        final long bitmapSize = getBitmapInternalSizeNet(netSize, peers, alStripes, alStripeSize, bitmapBlockSize);
        final long alSize = getAlSize(alStripes, alStripeSize);
        final long mdSize = ceilingAlign(bitmapSize + alSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
        long grossSize = ceilingAlign(netSize, bitmapBlockSize) + mdSize;

        checkMaxDrbdSize(grossSize);

        if (grossSize < DRBD_MIN_GROSS_kiB)
        {
            grossSize = DRBD_MIN_GROSS_kiB;
        }

        return grossSize;
    }


    @Override
    public long getInternalMdSize(
        final SizeSpec  mode,
        final long      size,
        final short     peers,
        final int       alStripes,
        final long      alStripeSize,
        final int       bitmapBlockSize
    )
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException
    {
        checkValid(size, peers, alStripes, alStripeSize, bitmapBlockSize);
        checkBitmapBlockSize(bitmapBlockSize);

        final long alignedSize = ceilingAlign(size, bitmapBlockSize);

        final long alSize = getAlSize(alStripes, alStripeSize);
        long mdSize = switch (mode)
        {
            case NET_SIZE ->
            {
                long bitmapSize =
                    getBitmapInternalSizeNet(alignedSize, peers, alStripes, alStripeSize, bitmapBlockSize);
                yield ceilingAlign(alSize + bitmapSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
            }
            case GROSS_SIZE ->
            {
                long bitmapSize = getBitmapInternalSizeGross(alignedSize, peers, bitmapBlockSize);
                yield ceilingAlign(alSize + bitmapSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);
            }
            default -> throw new IllegalArgumentException();
        };

        return mdSize;
    }


    @Override
    public long getExternalMdSize(
        final long      size,
        final short     peers,
        final int       alStripes,
        final long      alStripeSize,
        final int       bitmapBlockSize
    )
        throws MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException
    {
        checkValid(size, peers, alStripes, alStripeSize, bitmapBlockSize);
        checkBitmapBlockSize(bitmapBlockSize);

        checkMaxDrbdSize(size);
        checkPeers(peers);

        final long alignedSize = ceilingAlign(size, DRBD_DATA_ALIGN_kiB);

        final long alSize = getAlSize(alStripes, alStripeSize);
        final long bitmapSize = getBitmapExternalSize(alignedSize, peers, bitmapBlockSize);
        long mdSize = ceilingAlign(alSize + bitmapSize + DRBD_MD_SUPERBLK_kiB, DRBD_MD_ALIGN_kiB);

        return Math.max(mdSize, DRBD_MIN_EXT_MD_kiB);
    }


    private static long ceilingDivide(final long dividend, final long divisor)
    {
        long quotient = dividend / divisor;
        if (dividend % divisor != 0)
        {
            ++quotient;
        }
        return quotient;
    }


    private static long ceilingAlign(final long value, final long alignment)
    {
        long result = value;
        if (value % alignment != 0)
        {
            result = ((value / alignment) + 1) * alignment;
        }
        return result;
    }


    private static long floorAlign(final long value, final long alignment)
    {
        return (value / alignment) * alignment;
    }


    private static long getAlSize(
        final int   alStripes,
        final long  alStripeSize
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
        alSize = ceilingAlign(alSize, DRBD_AL_ALIGN_kiB);

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


    public static void checkPeers(final short peers) throws PeerCountException
    {
        if (peers < DRBD_MIN_PEERS || peers > DRBD_MAX_PEERS)
        {
            throw new PeerCountException();
        }
    }


    public static void checkBitmapBlockSize(final int bitmapBlockSize) throws BitmapBlockSizeException
    {
        // bitmapBlockSize must be in range and must also be a power of 2
        if (bitmapBlockSize < DRBD_MIN_BM_BIT_COVER_kiB || bitmapBlockSize > DRBD_MAX_BM_BIT_COVER_kiB ||
            Integer.bitCount(bitmapBlockSize) != 1)
        {
            throw new BitmapBlockSizeException();
        }
    }


    public static void checkMinDrbdSizeNet(final long netSize) throws MinSizeException
    {
        if (netSize < DRBD_MIN_NET_kiB)
        {
            throw new MinSizeException();
        }
    }


    public static void checkMinDrbdSizeGross(final long grossSize) throws MinSizeException
    {
        if (grossSize < DRBD_MIN_GROSS_kiB)
        {
            throw new MinSizeException();
        }
    }


    public static void checkMaxDrbdSize(final long size) throws MaxSizeException
    {
        if (size > DRBD_MAX_kiB)
        {
            throw new MaxSizeException();
        }
    }


    private static long getBitmapExternalSize(final long size, final short peers, final int bitmapBlockSize)
        throws MinSizeException, MaxSizeException, PeerCountException, BitmapBlockSizeException
    {
        checkMinDrbdSizeNet(size);
        checkMaxDrbdSize(size);
        checkPeers(peers);
        checkBitmapBlockSize(bitmapBlockSize);

        final long alignedSize = ceilingAlign(size, DRBD_DATA_ALIGN_kiB);

        final int byteCoverSize = bitmapBlockSize << 3;
        long bitmapPeerBytes = ceilingDivide(alignedSize, byteCoverSize);
        bitmapPeerBytes = ceilingAlign(bitmapPeerBytes, DRBD_BM_PEER_ALIGN);
        final long bitmapBytes = bitmapPeerBytes * peers;
        final long alignedBitmapBytes = ceilingAlign(bitmapBytes, DIVISOR_kiB);
        final long bitmapSize = ceilingAlign(alignedBitmapBytes / DIVISOR_kiB, DRBD_BM_ALIGN_kiB);

        return bitmapSize;
    }


    private static long getBitmapInternalSizeNet(
        final long      netSize,
        final short     peers,
        final int       alStripes,
        final long      alStripeSize,
        final int       bitmapBlockSize
    )
        throws MinSizeException, MaxSizeException,
               AlStripesException, MaxAlSizeException, MinAlSizeException,
               PeerCountException, BitmapBlockSizeException
    {
        checkMinDrbdSizeNet(netSize);
        checkMaxDrbdSize(netSize);
        checkPeers(peers);
        checkBitmapBlockSize(bitmapBlockSize);

        final long alignedNetSize = ceilingAlign(netSize, DRBD_DATA_ALIGN_kiB);

        final long alSize = getAlSize(alStripes, alStripeSize);

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
        long grossSize = alignedNetSize + alSize + DRBD_MD_SUPERBLK_kiB;
        long bitmapSize = 0;
        long bitmapCoverSize = 0;
        final int byteCoverSize = bitmapBlockSize << 3;
        while (bitmapCoverSize < grossSize)
        {
            // Bitmap size required to cover the gross size on each peer
            long bitmapPeerBytes = ceilingDivide(grossSize, byteCoverSize);
            // Align to the per-peer bitmap granularity
            bitmapPeerBytes = ceilingAlign(bitmapPeerBytes, DRBD_BM_PEER_ALIGN);

            // Bitmap size for all peers
            long bitmapBytes = bitmapPeerBytes * peers;

            // Gross size covered by the current bitmap size
            bitmapCoverSize = bitmapPeerBytes * byteCoverSize;

            // Actual size of the bitmap in the DRBD metadata area (after alignment)
            bitmapSize = ceilingAlign(ceilingAlign(bitmapBytes, DIVISOR_kiB) / DIVISOR_kiB, DRBD_BM_ALIGN_kiB);
            // Resulting gross size after including the bitmap size
            grossSize = alignedNetSize + ceilingAlign(alSize + DRBD_MD_SUPERBLK_kiB + bitmapSize, DRBD_DATA_ALIGN_kiB);
        }

        checkMaxDrbdSize(grossSize);

        return bitmapSize;
    }


    private static long getBitmapInternalSizeGross(final long grossSize, final short peers, final int bitmapBlockSize)
        throws MinSizeException, MaxSizeException, PeerCountException, BitmapBlockSizeException
    {
        checkMinDrbdSizeGross(grossSize);
        checkMaxDrbdSize(grossSize);
        checkPeers(peers);
        checkBitmapBlockSize(bitmapBlockSize);

        final long alignedGrossSize = ceilingAlign(grossSize, DRBD_DATA_ALIGN_kiB);

        final int byteCoverSize = bitmapBlockSize << 3;
        long bitmapPeerBytes = ceilingDivide(alignedGrossSize, byteCoverSize);
        bitmapPeerBytes = ceilingAlign(bitmapPeerBytes, DRBD_BM_PEER_ALIGN);
        final long bitmapBytes = bitmapPeerBytes * peers;
        final long alignedBitmapBytes = ceilingAlign(bitmapBytes, DIVISOR_kiB);
        final long bitmapSize = ceilingAlign(alignedBitmapBytes / DIVISOR_kiB, DRBD_BM_ALIGN_kiB);

        return bitmapSize;
    }
}
