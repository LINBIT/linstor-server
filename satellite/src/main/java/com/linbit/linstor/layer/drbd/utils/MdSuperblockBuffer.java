package com.linbit.linstor.layer.drbd.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

/**
 * Buffer for analyzing DRBD meta data superblock contents
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class MdSuperblockBuffer
{
    // DRBD's magic ID for detecting meta data
    // (DRBD 9.0.x)
    private static final int DRBD_MAGIC_ID = 0x8374026D;

    // Size of the DRBD superblock
    private static final int SUPERBLK_SIZE = 4096;

    // Bitwise AND mask for aligning to a 4k boundary
    private static final long ALIGN_4K_MASK = 0xFFFFFFFFFFFFF000L;

    // Values for generation identifier that indicate new metadata; that is, no initial sync has been performed
    private static final List<Long> INITIAL_GENERATION_OPTIONS = Arrays.asList(0x0L, 0x4L);

    private static final byte[] ZEROES = new byte[SUPERBLK_SIZE];
    private static final ByteBuffer ZEROES_BUFFER;

    static
    {
        Arrays.fill(ZEROES, (byte) 0);
        ZEROES_BUFFER = ByteBuffer.wrap(ZEROES);
        ZEROES_BUFFER.limit(ZEROES_BUFFER.capacity());
    }

    private byte[] data;
    private ByteBuffer buffer;

    private long mdEffectiveSize;
    private long mdCurrentGen;
    private long mdDeviceGen;
    private int mdFlags;
    private int mdMagic;
    private int mdSize;
    private int mdAlOffset;
    private int mdAlExtents;
    private int mdBitmapOffset;
    private int mdBitmapBitBlocksize;
    private int mdMaxBioSize;
    private int mdMaxPeers;
    private int mdNodeId;
    private int mdAlStripes;
    private int mdAlStripeSize;

    public MdSuperblockBuffer()
    {
        data = new byte[SUPERBLK_SIZE];
        buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);
    }

    public void readObject(final String objPath)
        throws IOException
    {
        clear();
        Path openPath = FileSystems.getDefault().getPath(objPath);
        try (FileChannel inChan = FileChannel.open(openPath, StandardOpenOption.READ))
        {
            long fileSize = inChan.size();

            // Align to a 4 kiB boundary
            fileSize &= ALIGN_4K_MASK;

            // Calculate the offset where to find the DRBD meta data superblock
            if (fileSize < SUPERBLK_SIZE)
            {
                throw new IOException("Object '" + objPath + "' is too small to contain a DRBD meta data superblock");
            }
            long offset = fileSize - SUPERBLK_SIZE;

            // Read the DRBD meta data superblock
            inChan.position(offset);
            inChan.read(buffer);
        }

        buffer.flip();

        mdEffectiveSize         = buffer.getLong();
        mdCurrentGen            = buffer.getLong();
        buffer.getLong(); // Reserved value slot, discard
        buffer.getLong(); // Reserved value slot, discard
        buffer.getLong(); // Reserved value slot, discard
        buffer.getLong(); // Reserved value slot, discard
        mdDeviceGen             = buffer.getLong();
        mdFlags                 = buffer.getInt();
        mdMagic                 = buffer.getInt();
        mdSize                  = buffer.getInt();
        mdAlOffset              = buffer.getInt();
        mdAlExtents             = buffer.getInt();
        mdBitmapOffset          = buffer.getInt();
        mdBitmapBitBlocksize    = buffer.getInt();
        mdMaxBioSize            = buffer.getInt();
        mdMaxPeers              = buffer.getInt();
        mdNodeId                = buffer.getInt();
        mdAlStripes             = buffer.getInt();
        mdAlStripeSize          = buffer.getInt();

        buffer.rewind();
    }

    public static void wipe(final String objPath) throws IOException
    {
        synchronized (ZEROES_BUFFER)
        {
            Path openPath = FileSystems.getDefault().getPath(objPath);
            try (FileChannel inChan = FileChannel.open(openPath, StandardOpenOption.WRITE))
            {
                long fileSize = inChan.size();

                // Align to a 4 kiB boundary
                fileSize &= ALIGN_4K_MASK;

                // Calculate the offset where to find the DRBD meta data superblock
                if (fileSize < SUPERBLK_SIZE)
                {
                    throw new IOException(
                        "Object '" + objPath + "' is too small to contain a DRBD meta data superblock"
                    );
                }
                long offset = fileSize - SUPERBLK_SIZE;

                // Read the DRBD meta data superblock
                inChan.position(offset);

                inChan.write(ZEROES_BUFFER);
            }
            finally
            {
                ZEROES_BUFFER.flip();
            }
        }
    }

    public boolean hasMetaData()
    {
        return mdMagic == DRBD_MAGIC_ID;
    }

    public boolean isMetaDataNew()
    {
        return INITIAL_GENERATION_OPTIONS.contains(mdCurrentGen);
    }

    public void clear()
    {
        buffer.clear();
        Arrays.fill(data, (byte) 0);

        mdEffectiveSize         = 0;
        mdCurrentGen            = 0;
        mdDeviceGen             = 0;
        mdFlags                 = 0;
        mdMagic                 = 0;
        mdSize                  = 0;
        mdAlOffset              = 0;
        mdAlExtents             = 0;
        mdBitmapOffset          = 0;
        mdBitmapBitBlocksize    = 0;
        mdMaxBioSize            = 0;
        mdMaxPeers              = 0;
        mdNodeId                = 0;
        mdAlStripes             = 0;
        mdAlStripeSize          = 0;
    }

    public long getEffectiveSize()
    {
        return mdEffectiveSize;
    }

    public long getCurrentGen()
    {
        return mdCurrentGen;
    }

    public long getDeviceGen()
    {
        return mdDeviceGen;
    }

    public int getFlags()
    {
        return mdFlags;
    }

    public int getMagic()
    {
        return mdMagic;
    }

    public int getSize()
    {
        return mdSize;
    }

    public int getAlOffset()
    {
        return mdAlOffset;
    }

    public int getAlExtents()
    {
        return mdAlExtents;
    }

    public int getBitmapOffset()
    {
        return mdBitmapOffset;
    }

    public int getBitmapBitBlocksize()
    {
        return mdBitmapBitBlocksize;
    }

    public int getMaxBioSize()
    {
        return mdMaxBioSize;
    }

    public int getMaxPeers()
    {
        return mdMaxPeers;
    }

    public int getNodeId()
    {
        return mdNodeId;
    }

    public int getAlStripes()
    {
        return mdAlStripes;
    }

    public int getAlStripeSize()
    {
        return mdAlStripeSize;
    }
}
