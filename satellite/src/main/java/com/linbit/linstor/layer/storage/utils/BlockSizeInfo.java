package com.linbit.linstor.layer.storage.utils;

import com.linbit.utils.MathUtils;
import com.linbit.utils.SymbolicLinkResolver;

import static com.linbit.linstor.layer.storage.BlockSizeConsts.DFLT_IO_SIZE;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MAX_IO_SIZE;
import static com.linbit.linstor.layer.storage.BlockSizeConsts.MIN_IO_SIZE;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;

public class BlockSizeInfo
{
    /**
     * Determines the blocksize, aka minimum I/O size, for the specified backing storage path.
     *
     * See getBlockSize(Path).
     *
     * @param storageObjPath Backing storage path
     * @return Block size, aka minimum I/O size, where 512L &lt;= blocksize &lt;= 4096L
     */
    public static long getBlockSize(final String storageObjPath)
    {
        final Path storageObj = Path.of(storageObjPath);
        return getBlockSize(storageObj);
    }

    /**
     * Determines the block size, aka minimum I/O size, for the specified backing storage path.
     *
     * If the specified path is a symbolic link, then an attempt is made to resolve symbolic links
     * until the actual block device special file is found. The file name of this file is them used
     * to find the <code>/sys/block/.../queue/physical_block_size</code> file in the Linux kernel pseudo-filesystem.
     * We deliberately do <b>not</b> use <code>minimum_io_size</code> since (by documentation, see
     * https://raw.githubusercontent.com/torvalds/linux/refs/heads/master/Documentation/ABI/stable/sysfs-block
     * for more information) that is the "preferred minimum I/O size".
     *
     * A default block size of IO_SIZE_DFLT is returned in case of a failure to determine a valid block size.
     *
     * @param storageObj Backing storage Path
     * @return Block size, aka minimum I/O size, where MIN_IO_SIZE &lt;= blocksize &lt;= MAX_IO_SIZE
     */
    public static long getBlockSize(final Path storageObj)
    {
        long blockSize = DFLT_IO_SIZE;
        try
        {
            final Path blockDevice = SymbolicLinkResolver.resolveSymLink(storageObj);
            final Path infoSourceName = blockDevice.getFileName();
            final Path infoSource = Path.of("/sys/block", infoSourceName.toString(), "queue/physical_block_size");

            final byte[] data = new byte[32];
            try (final FileInputStream fileIn = new FileInputStream(infoSource.toString()))
            {
                final int readCount = fileIn.read(data);
                if (readCount > 0)
                {
                    String numberStr = new String(data, 0, readCount);
                    numberStr = numberStr.trim();
                    try
                    {
                        final long minIoSize = Long.parseLong(numberStr);
                        blockSize = MathUtils.bounds(MIN_IO_SIZE, minIoSize, MAX_IO_SIZE);
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
        return blockSize;
    }
}
