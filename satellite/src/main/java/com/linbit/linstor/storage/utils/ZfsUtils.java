package com.linbit.linstor.storage.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

public class ZfsUtils
{
    private static final String DELIMITER = "\t"; // default for all "zfs -H ..." commands

    private static final int ZFS_LIST_COL_IDENTIFIER = 0;
    private static final int ZFS_LIST_COL_ALLOCATED_SIZE = 1;
    private static final int ZFS_LIST_COL_USABLE_SIZE = 2;
    private static final int ZFS_LIST_COL_TYPE = 3;

    private static final int ZFS_LIST_FILESYSTEMS_COL_IDENTIFIER = 0;
    private static final int ZFS_LIST_FILESYSTEMS_COL_AVAILABLE_SIZE = 1;
    private static final int ZFS_LIST_FILESYSTEMS_COL_TYPE = 2;

    private static final String ZFS_TYPE_VOLUME = "volume";
    private static final String ZFS_TYPE_SNAPSHOT = "snapshot";
    private static final String ZFS_TYPE_FILESYSTEM = "filesystem";

    private static final int ZFS_EXTENT_SIZE_COL_SIZE = 1;

    private ZfsUtils()
    {
    }

    public static class ZfsInfo
    {
        public final String poolName;
        public final String identifier;
        public final String type;
        public final String path;
        public final long allocatedSize;
        public final long usableSize;

        ZfsInfo(
            String poolNameRef,
            String identifierRef,
            String typeRef,
            String pathRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            poolName = poolNameRef;
            identifier = identifierRef;
            type = typeRef;
            path = pathRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
        }
    }

    public static HashMap<String, ZfsInfo> getThinZPoolsList(ExtCmd extCmd)
        throws StorageException
    {
        final OutputData output = ZfsCommands.listThinPools(extCmd);
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, ZfsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 3;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            if (data.length == expectedColCount)
            {
                final String identifier = data[ZFS_LIST_FILESYSTEMS_COL_IDENTIFIER];
                final String availableSizeStr = data[ZFS_LIST_FILESYSTEMS_COL_AVAILABLE_SIZE];
                final String type = data[ZFS_LIST_FILESYSTEMS_COL_TYPE];

                if (type.equals(ZFS_TYPE_FILESYSTEM))
                {
                    long usableSize;
                    try
                    {
                        usableSize = SizeConv.convert(
                            StorageUtils.parseDecimalAsLong(availableSizeStr.trim()),
                            SizeUnit.UNIT_B,
                            SizeUnit.UNIT_KiB
                        );
                    }
                    catch (NumberFormatException nfExc)
                    {
                        throw new StorageException(
                            "Unable to parse available volume size",
                            "Size to parse: '" + availableSizeStr + "'",
                            null,
                            null,
                            "External command used to query logical volume info: " +
                                String.join(" ", output.executedCommand),
                                nfExc
                        );
                    }
                    int poolNameEndIndex = identifier.lastIndexOf(File.separator);
                    if (poolNameEndIndex == -1)
                    {
                        poolNameEndIndex = identifier.length() - 1;
                    }
                    final ZfsInfo state = new ZfsInfo(
                        identifier.substring(0, poolNameEndIndex),
                        identifier.substring(poolNameEndIndex + 1),
                        type,
                        buildZfsPath(identifier),
                        -1,
                        usableSize
                    );
                    infoByIdentifier.put(identifier, state);
                }
            }
        }
        return infoByIdentifier;

    }

    public static HashMap<String, ZfsInfo> getZfsList(final ExtCmd extCmd)
        throws StorageException
    {
        final OutputData output = ZfsCommands.list(extCmd);
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, ZfsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 4;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            if (data.length == expectedColCount)
            {
                final String identifier = data[ZFS_LIST_COL_IDENTIFIER];
                final String allocatedSizeStr = data[ZFS_LIST_COL_ALLOCATED_SIZE];
                final String usableSizeStr = data[ZFS_LIST_COL_USABLE_SIZE];
                final String type = data[ZFS_LIST_COL_TYPE];

                if (type.equals(ZFS_TYPE_VOLUME) || type.equals(ZFS_TYPE_SNAPSHOT))
                {
                    long allocatedSize;
                    try
                    {
                        allocatedSize = SizeConv.convert(
                            StorageUtils.parseDecimalAsLong(allocatedSizeStr.trim()),
                            SizeUnit.UNIT_B,
                            SizeUnit.UNIT_KiB
                        );
                    }
                    catch (NumberFormatException nfExc)
                    {
                        throw new StorageException(
                            "Unable to parse allocated volume size",
                            "Size to parse: '" + allocatedSizeStr + "'",
                            null,
                            null,
                            "External command used to query logical volume info: " +
                                String.join(" ", output.executedCommand),
                                nfExc
                        );
                    }
                    long usableSize;
                    try
                    {
                        usableSize = SizeConv.convert(
                            StorageUtils.parseDecimalAsLong(usableSizeStr.trim()),
                            SizeUnit.UNIT_B,
                            SizeUnit.UNIT_KiB
                        );
                    }
                    catch (NumberFormatException nfExc)
                    {
                        throw new StorageException(
                            "Unable to parse usable volume size",
                            "Size to parse: '" + usableSizeStr + "'",
                            null,
                            null,
                            "External command used to query logical volume info: " +
                                String.join(" ", output.executedCommand),
                                nfExc
                        );
                    }

                    int poolNameEndIndex = identifier.lastIndexOf(File.separator);
                    if (poolNameEndIndex == -1)
                    {
                        poolNameEndIndex = identifier.length() - 1;
                    }
                    final ZfsInfo state = new ZfsInfo(
                        identifier.substring(0, poolNameEndIndex),
                        identifier.substring(poolNameEndIndex + 1),
                        type,
                        buildZfsPath(identifier),
                        allocatedSize,
                        usableSize
                    );
                    infoByIdentifier.put(identifier, state);
                }
            }
        }
        return infoByIdentifier;
    }

    public static Set<String> getZPoolList(ExtCmd extCmd) throws StorageException
    {
        final OutputData output = ZfsCommands.listZpools(extCmd);
        final String stdOut = new String(output.stdoutData);
        final Set<String> ret = new TreeSet<>();

        ret.addAll(Arrays.asList(stdOut.split("\n")));

        return ret;
    }


    private static String buildZfsPath(String identifier)
    {
        return File.separator + "dev" +
            File.separator + "zvol" +
            File.separator + identifier;
    }

    public static Map<String, Long> getZPoolFreeSize(ExtCmd extCmd, Set<String> zPool) throws StorageException
    {
        Map<String, Long> freeSizes = ParseUtils.parseSimpleTable(
            ZfsCommands.getZPoolFreeSize(extCmd, zPool),
            DELIMITER,
            "free size"
        );

        for (Entry<String, Long> entry : freeSizes.entrySet())
        {
            entry.setValue(
                SizeConv.convert(
                    entry.getValue(),
                    SizeUnit.UNIT_B,
                    SizeUnit.UNIT_KiB
                )
            );
        }

        return freeSizes;
    }

    public static long getZfsExtentSize(ExtCmd extCmd, String poolName, String identifier) throws StorageException
    {
        return ParseUtils.parseDecimalAsLong(
            new String(
                ZfsCommands.getExtentSize(extCmd, poolName, identifier)
                    .stdoutData
            ).trim()
        );
    }

    public static Map<String, Long> getZPoolTotalSize(ExtCmd extCmd, Set<String> zPools) throws StorageException
    {
        Map<String, Long> totalSizes = ParseUtils.parseSimpleTable(
            ZfsCommands.getZPoolTotalSize(extCmd, zPools),
            DELIMITER,
            "free size",
            0, // field for name
            2 // field for value
        );
        for (Entry<String, Long> entry : totalSizes.entrySet())
        {
            entry.setValue(
                SizeConv.convert(
                    entry.getValue(),
                    SizeUnit.UNIT_B,
                    SizeUnit.UNIT_KiB
                )
            );
        }
        return totalSizes;
    }
}
