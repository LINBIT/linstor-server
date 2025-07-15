package com.linbit.linstor.layer.storage.zfs.utils;

import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.layer.storage.utils.ParseUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands.ZfsVolumeType;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageUtils;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.utils.StringUtils;

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
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZfsUtils
{
    // cloning easily takes minutes if not longer. We can wait a few more seconds if ZFS still needs time..
    private static final int ZFS_DELETE_IF_EXISTS_RETRY_COUNT = 10;
    private static final int ZFS_DESTROY_RETRY_DELAY_IN_MS = 1_000;

    private static final String DELIMITER = "\t"; // default for all "zfs -H ..." commands

    private static final int ZFS_LIST_COL_IDENTIFIER        = 0; // -o "name"
    private static final int ZFS_LIST_COL_REFER_SIZE        = 1; // -o "refer"
    private static final int ZFS_LIST_COL_VOLSIZE           = 2; // -o "volsize"
    private static final int ZFS_LIST_COL_TYPE              = 3; // -o "type"
    private static final int ZFS_LIST_COL_VOL_BLOCK_SIZE    = 4; // -o "volblocksize"
    private static final int ZFS_LIST_COL_ORIGIN            = 5; // -o "origin"
    private static final int ZFS_LIST_COL_CLONES            = 6; // -o "clones"

    private static final int ZFS_LIST_FILESYSTEMS_COL_IDENTIFIER = 0;
    private static final int ZFS_LIST_FILESYSTEMS_COL_AVAILABLE_SIZE = 1;
    private static final int ZFS_LIST_FILESYSTEMS_COL_TYPE = 2;

    private static final int ZFS_GET_COL_NAME = 0;
    private static final int ZFS_GET_COL_VALUE = 1;

    private static final String ZFS_TYPE_VOLUME = "volume";
    private static final String ZFS_TYPE_SNAPSHOT = "snapshot";
    private static final String ZFS_TYPE_FILESYSTEM = "filesystem";

    private static final Pattern ZFS_CREATE_DRYRUN_OUTPUT_PATTERN = Pattern.compile("property\\s+volsize\\s+(\\d+)");

    private ZfsUtils()
    {
    }

    public static class ZfsInfo
    {
        public final String poolName;
        public final String identifier;
        public final String typeStr;
        public final ZfsVolumeType type;
        public final String path;
        public final long allocatedSize;
        public final long usableSize;
        // Snapshots do not report volBlockSize
        public final @Nullable Long volBlockSize;
        public final @Nullable String originStr;
        public final @Nullable String[] clonesStrArr;

        // will be filled in updateStates method
        public final List<ZfsInfo> snapshots = new ArrayList<>();
        public final List<ZfsInfo> clones = new ArrayList<>();

        // properties, will be filled after object is initialized
        public boolean markedForDeletion = false;

        ZfsInfo(
            String poolNameRef,
            String identifierRef,
            String typeRef,
            String pathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            @Nullable Long volBlockSizeRef,
            String originStrRef,
            String[] clonesArrRef
        )
            throws StorageException
        {
            poolName = poolNameRef;
            identifier = identifierRef;
            typeStr = typeRef;
            type = ZfsVolumeType.parseOrThrow(typeRef);
            path = pathRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            volBlockSize = volBlockSizeRef;
            originStr = originStrRef;
            clonesStrArr = clonesArrRef;
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
                            usableSize,
                            null,
                            null,
                            new String[0]
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

    public static HashMap<String, ZfsInfo> getZfsList(
        final ExtCmdFactory extCmdFactoryRef,
        Collection<String> datasets,
        DeviceProviderKind kindRef,
        Map<String, BiConsumer<ZfsInfo, String>> zfsPropertiesWithSetters
    )
        throws StorageException
    {
        final HashMap<String, ZfsInfo> infoByIdentifier = buildZfsInfoMap(extCmdFactoryRef.create(), datasets, kindRef);
        for (Entry<String, BiConsumer<ZfsInfo, String>> entry : zfsPropertiesWithSetters.entrySet())
        {
            Map<String, String> properties = getZfsLocalProperty(
                extCmdFactoryRef.create(),
                entry.getKey(),
                datasets,
                Function.identity()
            );

            BiConsumer<ZfsInfo, String> setter = entry.getValue();
            for (Map.Entry<String, String> propEntry : properties.entrySet())
            {
                @Nullable ZfsInfo zfsInfo = infoByIdentifier.get(propEntry.getKey());
                if (zfsInfo != null)
                {
                    setter.accept(zfsInfo, propEntry.getValue());
                }
            }
        }
        return infoByIdentifier;
    }

    private static HashMap<String, ZfsInfo> buildZfsInfoMap(
        final ExtCmd extCmd,
        Collection<String> datasets,
        DeviceProviderKind kindRef
    )
        throws StorageException
    {
        final OutputData output = ZfsCommands.list(extCmd, datasets);
        final String stdOut = new String(output.stdoutData);

        final HashMap<String, ZfsInfo> infoByIdentifier = new HashMap<>();
        final HashMap<String, ArrayList<ZfsInfo>> unprocessedSnapshots = new HashMap<>();
        final HashMap<String, ArrayList<ZfsInfo>> unprocessedZvols = new HashMap<>();

        final String[] lines = stdOut.split("\n");
        final int expectedColCount = 7;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            try
            {
                // older ZFS versions (< v2.0.5) do not show "-" on the "clones" column of snapshots if the snapshot
                // does not have clones. if the snapshot does have clones, the "clones" column works properly.
                boolean rowValid = data.length == expectedColCount || data.length == expectedColCount - 1;
                if (rowValid)
                {
                    final String identifier = data[ZFS_LIST_COL_IDENTIFIER];
                    final String usableSizeStr = data[ZFS_LIST_COL_VOLSIZE];
                    final String type = data[ZFS_LIST_COL_TYPE];
                    final String volBlockSizeStr = data[ZFS_LIST_COL_VOL_BLOCK_SIZE];
                    final String originStr = data[ZFS_LIST_COL_ORIGIN];
                    final @Nullable String clonesStr;
                    if (data.length == expectedColCount)
                    {
                        clonesStr = data[ZFS_LIST_COL_CLONES];
                    }
                    else
                    {
                        if (type.equals(ZFS_TYPE_SNAPSHOT))
                        {
                            clonesStr = "-";
                        }
                        else
                        {
                            clonesStr = null; // invalid, this row will be skipped
                            rowValid = false;
                        }
                    }

                    if (rowValid)
                    {
                        final String allocatedSizeStr;
                        if (kindRef == DeviceProviderKind.ZFS_THIN || type.equals(ZFS_TYPE_SNAPSHOT))
                        {
                            /*
                             * "refer" column shows the allocation size on disk. This size will grow for thick and thin
                             * volumes as well as for snapshots while using
                             */
                            allocatedSizeStr = data[ZFS_LIST_COL_REFER_SIZE];
                        }
                        else
                        {
                            /*
                             * "volsize" represents the size which was used during "zfs create -V ..." regardless if
                             * thin or thick volume
                             */
                            allocatedSizeStr = data[ZFS_LIST_COL_VOLSIZE]; // -o "volsize"
                        }

                        if (type.equals(ZFS_TYPE_VOLUME) || type.equals(ZFS_TYPE_SNAPSHOT))
                        {
                            String allocateByteSizeStr = allocatedSizeStr;
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

                            @Nullable Long volBlockSize;
                            if (volBlockSizeStr.trim().equals("-"))
                            {
                                // snapshot
                                volBlockSize = null;
                            }
                            else
                            {
                                volBlockSize = SizeConv.convert(
                                    StorageUtils.parseDecimalAsLong(volBlockSizeStr.trim()),
                                    SizeUnit.UNIT_B,
                                    SizeUnit.UNIT_KiB
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
                                usableSize,
                                volBlockSize,
                                originStr.equals("-") ? null : originStr,
                                clonesStr.equals("-") ? new String[0] : clonesStr.split(",")
                            );
                            infoByIdentifier.put(identifier, state);

                            if (type.equals(ZFS_TYPE_SNAPSHOT))
                            {
                                String baseZvolIdentifier = identifier.substring(0, identifier.indexOf("@"));
                                @Nullable ZfsInfo baseZvolInfo = infoByIdentifier.get(baseZvolIdentifier);
                                if (baseZvolInfo == null)
                                {
                                    unprocessedSnapshots
                                        .computeIfAbsent(baseZvolIdentifier, ignored -> new ArrayList<>())
                                        .add(state);
                                }
                                else
                                {
                                    // technically we are only storing "<baseZvol> has snapshot <state>" but not
                                    // "<state> is snapshot of <baseZvol>"
                                    baseZvolInfo.snapshots.add(state);
                                }
                            }
                            else
                            {
                                if (state.originStr != null)
                                {
                                    @Nullable ZfsInfo baseSnapInfo = infoByIdentifier.get(state.originStr);
                                    if (baseSnapInfo == null)
                                    {
                                        unprocessedZvols.computeIfAbsent(state.originStr, ignored -> new ArrayList<>())
                                            .add(state);
                                    }
                                    else
                                    {
                                        // technically we are only storing "<baseSnap> has clone <state>" but not
                                        // "<clone> is clone of <baseSnap>"
                                        baseSnapInfo.clones.add(state);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (NumberFormatException ignored)
            {
                // we could not parse a number so we ignore the whole line
            }
        }
        for (Map.Entry<String, ArrayList<ZfsInfo>> unprocessedSnapshot : unprocessedSnapshots.entrySet())
        {
            String baseZvolIdentifier = unprocessedSnapshot.getKey();
            ArrayList<ZfsInfo> snapshots = unprocessedSnapshot.getValue();
            @Nullable ZfsInfo baseZvolInfo = infoByIdentifier.get(baseZvolIdentifier);
            if (baseZvolInfo == null)
            {
                StringBuilder errorMsgBuilder = new StringBuilder(baseZvolIdentifier)
                    .append(" not found, but has ");
                if (snapshots.size() == 1)
                {
                    errorMsgBuilder.append("a snapshot");
                }
                else
                {
                    errorMsgBuilder.append("snapshots");
                }
                errorMsgBuilder.append(":");
                for (ZfsInfo snap : snapshots)
                {
                    errorMsgBuilder.append("\n * ")
                        .append(snap.poolName)
                        .append(File.separator)
                        .append(snap.identifier);
                }
                throw new StorageException(errorMsgBuilder.toString());
            }
            else
            {
                baseZvolInfo.snapshots.addAll(snapshots);
            }
        }
        for (Map.Entry<String, ArrayList<ZfsInfo>> unprocessedZvol : unprocessedZvols.entrySet())
        {
            String baseSnapIdentifier = unprocessedZvol.getKey();
            ArrayList<ZfsInfo> zvols = unprocessedZvol.getValue();
            @Nullable ZfsInfo baseSnapInfo = infoByIdentifier.get(baseSnapIdentifier);
            if (baseSnapInfo == null)
            {
                StringBuilder errorMsgBuilder = new StringBuilder(baseSnapIdentifier).append(
                    " not found, but has "
                );
                if (zvols.size() == 1)
                {
                    errorMsgBuilder.append("a clone");
                }
                else
                {
                    errorMsgBuilder.append("clones");
                }
                errorMsgBuilder.append(":");
                for (ZfsInfo zvol : zvols)
                {
                    errorMsgBuilder.append("\n * ")
                        .append(zvol.poolName)
                        .append(File.separator)
                        .append(zvol.identifier);
                }
                throw new StorageException(errorMsgBuilder.toString());
            }
            else
            {
                baseSnapInfo.clones.addAll(zvols);
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

    public static <T> Map<String, T> getZfsLocalProperty(
        ExtCmd extCmdRef,
        String zfsPropRef,
        Collection<String> dataSetsRef,
        Function<String, T> mappingFunctionRef
    )
        throws StorageException
    {
        final OutputData output = ZfsCommands.getUserProperty(extCmdRef, zfsPropRef, dataSetsRef);
        final HashMap<String, T> ret = new HashMap<>();

        final String[] lines = new String(output.stdoutData).split("\n");
        final int expectedColCount = 2;
        for (final String line : lines)
        {
            final String[] data = line.trim().split(DELIMITER);
            if (data.length == expectedColCount)
            {
                final String identifier = data[ZFS_GET_COL_NAME];
                final String value = data[ZFS_GET_COL_VALUE];

                ret.put(identifier, mappingFunctionRef.apply(value));
            }
        }

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

    /**
     * Executes a '<code>zfs create -n -P -V 1B $zpool/$dummy_volume</code>' and returns the reported
     * <code>volsize</code>.
     */
    public static long getZfsExtentSize(ExtCmd extCmd, String poolName) throws StorageException
    {
        OutputData outputData = ZfsCommands.create(
            extCmd,
            poolName,
            "LINSTOR_dry_run_" + UUID.randomUUID().toString(),
            1,
            true,
            "-n", // dry-run
            "-P" // parseable
        );

        long ret;

        String dryrunOutput = new String(outputData.stdoutData).trim();
        Matcher matcher = ZFS_CREATE_DRYRUN_OUTPUT_PATTERN.matcher(dryrunOutput);
        if (matcher.find())
        {
            String volSizeStr = matcher.group(1);
            ret = SizeConv.convert(Long.parseLong(volSizeStr), SizeUnit.UNIT_B, SizeUnit.UNIT_KiB);
        }
        else
        {
            throw new StorageException(
                "Unexpected output of '" + StringUtils.join(" ", outputData.executedCommand) +
                    "'. Could not find pattern: '" + ZFS_CREATE_DRYRUN_OUTPUT_PATTERN.pattern() +
                    "'.\nStandard out: \n" + dryrunOutput +
                    "\n\nStandard error: \n" + new String(outputData.stderrData)
            );
        }

        return ret;
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

    /**
     * Attempts a "zfs destroy ...", retries if necessary (with delays between attempts) until either exit code is 0
     * or error message suggests that the ZFS identifier (ZVOL or snapshot) no longer exists.
     * Will only throw Exception after max retry failed attempts.
     */
    public static void deleteIfExists(
        ExtCmd extCmdRef,
        String zpoolRef,
        String zfsIdentifierRef,
        ZfsCommands.ZfsVolumeType zfsTypeRef
    )
        throws StorageException
    {
        ZfsCommands.delete(
            extCmdRef,
            zpoolRef,
            zfsIdentifierRef,
            zfsTypeRef,
            new Commands.RetryHandler()
            {
                private int retryCount = ZFS_DELETE_IF_EXISTS_RETRY_COUNT;
                @Override
                public boolean skip(OutputData outDataRef)
                {
                    String stdErr = new String(outDataRef.stderrData);
                    return stdErr.contains("dataset does not exist") ||
                        stdErr.contains("could not find any snapshots to destroy; check snapshot names.");
                }

                @Override
                public boolean retry(OutputData outputDataRef)
                {
                    try
                    {
                        Thread.sleep(ZFS_DESTROY_RETRY_DELAY_IN_MS);
                    }
                    catch (InterruptedException exc)
                    {
                        retryCount = 0;
                        Thread.currentThread().interrupt();
                    }
                    return retryCount-- > 0;
                }
            }
        );
    }
}
