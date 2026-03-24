package com.linbit.linstor.layer.storage.utils;

import com.linbit.utils.MathUtils;
import com.linbit.utils.SymbolicLinkResolver;

import static com.linbit.linstor.layer.storage.BlockSizeConsts.DFLT_DISC_GRAN;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.DFLT_OPT_IO_SIZE;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.DFLT_PHY_IO_SIZE;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MAX_DISC_GRAN;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MAX_OPT_IO_SIZE;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MAX_PHY_IO_SIZE;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MIN_DISC_GRAN;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MIN_OPT_IO_SIZE;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MIN_PHY_IO_SIZE;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class BlockSizeInfo
{
    private static final int DFLT_BUF_SIZE_FOR_NUMBERS = 32;
    private static final String QUEUE_PHY_BLK_SIZE = "queue/physical_block_size";
    private static final String QUEUE_OPT_IO_SIZE = "queue/optimal_io_size";
    private static final String QUEUE_DISC_GRAN = "queue/discard_granularity";

    /**
     * Determines the blocksize, aka minimum I/O size, for the specified backing storage path.
     *
     * See {@link #getPhysicalBlockSize(Path)}.
     *
     * @param storageObjPath Backing storage path
     * @return Block size, aka minimum I/O size, where 512L &lt;= blocksize &lt;= 4096L
     */
    public static long getPhysicalBlockSize(final String storageObjPath)
    {
        final Path storageObj = Path.of(storageObjPath);
        return getPhysicalBlockSize(storageObj);
    }

    /**
     * <p>Determines the block size, aka minimum I/O size, for the specified backing storage path.</p>
     *
     * <p>If the specified path is a symbolic link, then an attempt is made to resolve symbolic links
     * until the actual block device special file is found. The file name of this file is them used
     * to find the <code>/sys/block/.../queue/physical_block_size</code> file in the Linux kernel pseudo-filesystem.
     * We deliberately do <b>not</b> use <code>minimum_io_size</code> since (by documentation, see
     * https://raw.githubusercontent.com/torvalds/linux/refs/heads/master/Documentation/ABI/stable/sysfs-block
     * for more information) that is the "preferred minimum I/O size".</p>
     *
     * <p>A default block size of IO_SIZE_DFLT is returned in case of a failure to determine a valid block size.</p>
     *
     * @param storageObjRef Backing storage Path
     * @return Block size, aka minimum I/O size, where MIN_IO_SIZE &lt;= blocksize &lt;= MAX_IO_SIZE
     */
    public static long getPhysicalBlockSize(final Path storageObjRef)
    {
        return getSize(storageObjRef, QUEUE_PHY_BLK_SIZE, DFLT_PHY_IO_SIZE, MIN_PHY_IO_SIZE, MAX_PHY_IO_SIZE);
    }

    /**
     * Returns <code>/sys/block/.../queue/optimal_io_size</code> of the given device.
     *
     */
    public static long getOptimalIoSize(final Path storageObjRef)
    {
        return getSize(storageObjRef, QUEUE_OPT_IO_SIZE, DFLT_OPT_IO_SIZE, MIN_OPT_IO_SIZE, MAX_OPT_IO_SIZE);
    }

    /**
     * Returns <code>/sys/block/.../queue/discard_granularity</code> of the given device.
     * A value of 0 means the device does not support discard operations.
     */
    public static long getDiscardGranularity(final Path storageObjRef)
    {
        return getSize(storageObjRef, QUEUE_DISC_GRAN, DFLT_DISC_GRAN, MIN_DISC_GRAN, MAX_DISC_GRAN);
    }

    private static long getSize(
        final Path storageObjRef,
        final String queueIdRef,
        final long dfltValRef,
        final long minValRef,
        final long maxValRef
    )
    {
        long ret = dfltValRef;
        try
        {
            final Path blockDevice = SymbolicLinkResolver.resolveSymLink(storageObjRef);
            final Path infoSourceName = blockDevice.getFileName();
            final Path infoSource = Path.of("/sys/block", infoSourceName.toString(), queueIdRef);

            final byte[] data = new byte[DFLT_BUF_SIZE_FOR_NUMBERS];
            try (FileInputStream fileIn = new FileInputStream(infoSource.toString()))
            {
                final int readCount = fileIn.read(data);
                if (readCount > 0)
                {
                    String numberStr = new String(data, 0, readCount, StandardCharsets.UTF_8);
                    numberStr = numberStr.trim();
                    try
                    {
                        final long unboundedSize = Long.parseLong(numberStr);
                        ret = MathUtils.bounds(minValRef, unboundedSize, maxValRef);
                    }
                    catch (NumberFormatException ignored)
                    {
                    }
                }
            }
        }
        catch (IOException ignored)
        {
        }
        return ret;
    }
}
