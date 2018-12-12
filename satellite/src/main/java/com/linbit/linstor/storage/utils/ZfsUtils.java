package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ZfsUtils
{
    private static final int KIB = 1024;

    private static final String DELIMITER = "\t"; // default for all "zfs -H ..." commands

    private static final int ZFS_LIST_COL_IDENTIFIER = 0;
    private static final int ZFS_LIST_COL_SIZE = 1;
    private static final int ZFS_LIST_COL_TYPE = 2;

    private static final String ZFS_TYPE_VOLUME = "volume";
    private static final String ZFS_TYPE_SNAPSHOT = "snapshot";

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
        public final long size;

        ZfsInfo(String poolNameRef, String identifierRef, String typeRef, String pathRef, long sizeRef)
        {
            poolName = poolNameRef;
            identifier = identifierRef;
            type = typeRef;
            path = pathRef;
            size = sizeRef;
        }
    }

    public static HashMap<String, ZfsInfo> getZfsList(final ExtCmd extCmd)
        throws StorageException
    {
        final OutputData output = ZfsCommands.list(extCmd);
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, ZfsInfo> infoByIdentifier = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 3;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            if (data.length == expectedColCount)
            {
                final String identifier = data[ZFS_LIST_COL_IDENTIFIER];
                final String sizeStr = data[ZFS_LIST_COL_SIZE];
                final String type = data[ZFS_LIST_COL_TYPE];

                if (type.equals(ZFS_TYPE_VOLUME) || type.equals(ZFS_TYPE_SNAPSHOT))
                {
                    long size;
                    try
                    {
                        size = StorageUtils.parseDecimalAsLong(sizeStr.trim());
                        size /= KIB; // zfs list returns size in bytes
                    }
                    catch (NumberFormatException nfExc)
                    {
                        throw new StorageException(
                            "Unable to parse logical volume size",
                            "Size to parse: '" + sizeStr + "'",
                            null,
                            null,
                            "External command used to query logical volume info: " +
                                String.join(" ", output.executedCommand),
                            nfExc
                        );
                    }

                    int poolNameEndIndex = identifier.lastIndexOf(File.separator);
                    final ZfsInfo state = new ZfsInfo(
                        identifier.substring(0, poolNameEndIndex),
                        identifier.substring(poolNameEndIndex + 1),
                        type,
                        buildZfsPath(identifier),
                        size
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
        return ParseUtils.parseSimpleTable(
            ZfsCommands.getZPoolFreeSize(extCmd, zPool),
            DELIMITER,
            "free size"
        );
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
        return ParseUtils.parseSimpleTable(
            ZfsCommands.getZPoolTotalSize(extCmd, zPools),
            DELIMITER,
            "free size",
            0, // field for name
            2 // field for value
        );
    }
}
