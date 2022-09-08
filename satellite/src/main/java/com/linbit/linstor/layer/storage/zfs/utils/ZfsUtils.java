package com.linbit.linstor.layer.storage.zfs.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.layer.storage.utils.ParseUtils;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

public class ZfsUtils
{
    private static final String DELIMITER = "\t"; // default for all "zfs -H ..." commands

    private static final int ZFS_LIST_COL_IDENTIFIER = 0;
    private static final int ZFS_LIST_COL_USED_SIZE = 1;
    private static final int ZFS_LIST_COL_REFER_SIZE = 2;
    private static final int ZFS_LIST_COL_USABLE_SIZE = 3;
    private static final int ZFS_LIST_COL_TYPE = 4;

    private static final int ZFS_LIST_FILESYSTEMS_COL_IDENTIFIER = 0;
    private static final int ZFS_LIST_FILESYSTEMS_COL_AVAILABLE_SIZE = 1;
    private static final int ZFS_LIST_FILESYSTEMS_COL_TYPE = 2;

    private static final String ZFS_TYPE_VOLUME = "volume";
    private static final String ZFS_TYPE_SNAPSHOT = "snapshot";
    private static final String ZFS_TYPE_FILESYSTEM = "filesystem";

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

    public static HashMap<String, ZfsInfo> getThinZPoolsList(ExtCmd extCmd, Collection<String> datasets)
        throws StorageException
    {
        final OutputData output = ZfsCommands.listThinPools(extCmd, datasets);
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, ZfsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 3;
        for (final String line : lines)
        {
            try
            {
                final String[] data = line.trim().split(DELIMITER);
                if (data.length == expectedColCount)
                {
                    final String identifier = data[ZFS_LIST_FILESYSTEMS_COL_IDENTIFIER];
                    final String availableSizeStr = data[ZFS_LIST_FILESYSTEMS_COL_AVAILABLE_SIZE];
                    final String type = data[ZFS_LIST_FILESYSTEMS_COL_TYPE];

                    if (type.equals(ZFS_TYPE_FILESYSTEM))
                    {
                        long usableSize = SizeConv.convert(
                            StorageUtils.parseDecimalAsLong(availableSizeStr.trim()),
                            SizeUnit.UNIT_B,
                            SizeUnit.UNIT_KiB
                        );
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
            catch (NumberFormatException ignored)
            {
                // we could not parse a number so we ignore the whole line
            }
        }
        return infoByIdentifier;

    }

    public static HashMap<String, ZfsInfo> getZfsList(final ExtCmd extCmd, Collection<String> datasets)
        throws StorageException
    {
        final OutputData output = ZfsCommands.list(extCmd, datasets);
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, ZfsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 5;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            try
            {
                if (data.length == expectedColCount)
                {
                    final String identifier = data[ZFS_LIST_COL_IDENTIFIER];
                    final String snapSizeStr = data[ZFS_LIST_COL_USED_SIZE]; // USED size seems to be right for snapshot
                    final String allocatedSizeStr = data[ZFS_LIST_COL_REFER_SIZE]; // volumes use REFER
                    final String usableSizeStr = data[ZFS_LIST_COL_USABLE_SIZE];
                    final String type = data[ZFS_LIST_COL_TYPE];

                    if (type.equals(ZFS_TYPE_VOLUME) || type.equals(ZFS_TYPE_SNAPSHOT))
                    {
                        String allocateByteSizeStr = type.equals(ZFS_TYPE_SNAPSHOT) ? snapSizeStr : allocatedSizeStr;
                        long allocatedSize = SizeConv.convert(
                            StorageUtils.parseDecimalAsLong(allocateByteSizeStr.trim()),
                            SizeUnit.UNIT_B,
                            SizeUnit.UNIT_KiB
                        );

                        long usableSize = SizeConv.convert(
                            StorageUtils.parseDecimalAsLong(usableSizeStr.trim()),
                            SizeUnit.UNIT_B,
                            SizeUnit.UNIT_KiB
                        );

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
            catch (NumberFormatException ignored)
            {
                // we could not parse a number so we ignore the whole line
            }
        }
        return infoByIdentifier;
    }

    public static Set<String> getZPoolList(ExtCmd extCmd) throws StorageException
    {
        final OutputData output = ZfsCommands.listZpools(extCmd);
        final String stdOut = new String(output.stdoutData);

        return new TreeSet<>(Arrays.asList(stdOut.split("\n")));
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

    public static String getZPoolRootName(String zpoolDatasetName)
    {
        int idx = zpoolDatasetName.indexOf(File.separator);
        if (idx == -1)
        {
            idx = zpoolDatasetName.length();
        }
        return zpoolDatasetName.substring(0, idx);
    }

    public static Map<String, Long> getZPoolTotalSize(ExtCmd extCmd, Set<String> zPools) throws StorageException
    {
        final Map<String, Long> quotaSizes = ParseUtils.parseSimpleTable(
            ZfsCommands.getQuotaSize(extCmd, zPools),
            DELIMITER,
            "quota size",
            0, // field for name
            1 // field for value
        );

        Set<String> zpoolRoots = new HashSet<>();
        for (String zpoolName : zPools)
        {
            zpoolRoots.add(getZPoolRootName(zpoolName));
        }

        Map<String, Long> totalSizes = ParseUtils.parseSimpleTable(
            ZfsCommands.getZPoolTotalSize(extCmd, zpoolRoots),
            DELIMITER,
            "free size",
            0, // field for name
            2 // field for value
        );
        for (Entry<String, Long> entry : quotaSizes.entrySet())
        {
            Long sizeValue = entry.getValue() == 0L ?
                totalSizes.get(getZPoolRootName(entry.getKey())) : entry.getValue();
            entry.setValue(
                SizeConv.convert(
                    sizeValue,
                    SizeUnit.UNIT_B,
                    SizeUnit.UNIT_KiB
                )
            );
        }
        return quotaSizes;
    }

    public static List<String> getPhysicalVolumes(ExtCmd extCmd, String zPoolRef) throws StorageException
    {
        List<String> devices = new ArrayList<>();

        OutputData out = ZfsCommands.getPhysicalDevices(extCmd, zPoolRef);
        String outStr = new String(out.stdoutData);
        String[] lines = outStr.split("\n");
        // first line is only the name of the zpool
        for (int idx = 1; idx < lines.length; ++idx)
        {
            String line = lines[idx].trim();
            String[] subColumns = line.split("\t");
            devices.add(subColumns[0]);
        }
        return devices;
    }
}
