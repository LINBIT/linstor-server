package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.utils.StringUtils;

import javax.annotation.Nonnull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class LsBlkUtils
{
    private static final long MINIMAL_DEVICE_SIZE_BYTES = 1024 * 1024 * 1024;

    private static final int MAJOR_DRBD_NR = 147;

    private LsBlkUtils()
    {
    }

    private static List<String> splitFields(final String line)
    {
        ArrayList<String> fields = new ArrayList<>();
        int lastEnd = 0;
        boolean inString = false;
        for (int idx = 0; idx < line.length(); idx++)
        {
            if (line.charAt(idx) == '"')
            {
                // zfs and lv do not allow volume names with " in it, so do not handle escaping
                if (inString)
                {
                    // field end reached, cut out
                    inString = false;
                    fields.add(line.substring(lastEnd, idx + 1));
                    if (!(idx + 2 >= line.length() || line.charAt(idx + 1) == ' '))
                    {
                        throw new RuntimeException(
                            String.format("Unexpected character '%c' after field '%s'.",
                                line.charAt(idx + 1),
                                fields.get(fields.size() - 1)
                            )
                        );
                    }
                    lastEnd = idx + 2;
                }
                else
                {
                    inString = true;
                }
            }
        }
        return fields;
    }

    static List<LsBlkEntry> parseLsblkOutput(String lsblkOutput)
    {
        ArrayList<LsBlkEntry> lsBlkEntries = new ArrayList<>();
        for (String line : lsblkOutput.split("\n"))
        {
            List<String> fields = splitFields(line.trim());
            if (fields.size() > 0)
            {
                lsBlkEntries.add(new LsBlkEntry(fields));
            }
        }
        return lsBlkEntries;
    }

    public static List<LsBlkEntry> lsblk(ExtCmd extCmd) throws StorageException
    {
        return lsblk(extCmd, null);
    }

    public static List<LsBlkEntry> lsblk(ExtCmd extCmd, @Nullable String devicePathRef)
        throws StorageException
    {
        String[] command = new String[]{
            "lsblk",
            "-P",
            "-b",
            "--paths",
            "-o", Arrays.stream(LsBlkEntry.LsBlkFields.values())
                .map(LsBlkEntry.LsBlkFields::toString)
                .collect(Collectors.joining(","))
        };
        if (devicePathRef != null)
        {
            command = StringUtils.concat(command, devicePathRef);
        }
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd.setSaveWithoutSharedLocks(true),
            command,
            "Failed execute lsblk",
            "Failed to execute lsblk"
        );

        return parseLsblkOutput(new String(outputData.stdoutData, StandardCharsets.UTF_8));
    }

    public static boolean parentIsVDO(ExtCmd extCmd, @Nonnull List<String> devicePathList)
        throws StorageException
    {
        // TODO: If possible, it would make sense time-complexity-wise to call the lsblk method
        //       only once for multiple calls of this method, and have it create a map of
        //       names to LsBlkEntry items, so this method could perform a map lookup rather
        //       than having to recreate and then iterate the list twice for each call.
        boolean vdoFlag = !devicePathList.isEmpty();
        if (vdoFlag)
        {
            List<LsBlkEntry> entries = lsblk(extCmd);
            Iterator<String> devicePathIter = devicePathList.iterator();
            while (devicePathIter.hasNext() && vdoFlag)
            {
                final String devicePath = devicePathIter.next();
                LsBlkEntry dev = getLsBlkEntryByName(entries, devicePath);
                vdoFlag = dev != null;
                if (vdoFlag)
                {
                    LsBlkEntry parent = getLsBlkEntryByName(entries, dev.getParentName());
                    vdoFlag = parent != null;
                    if (vdoFlag)
                    {
                        vdoFlag = parent.getFsType().equalsIgnoreCase("vdo");
                    }
                }
            }
        }
        return vdoFlag;
    }

    private static LsBlkEntry getLsBlkEntryByName(final List<LsBlkEntry> entries, final String name)
    {
        LsBlkEntry selectedEntry = null;
        Iterator<LsBlkEntry> entryIter = entries.iterator();
        while (entryIter.hasNext() && selectedEntry == null)
        {
            final LsBlkEntry entry = entryIter.next();
            final String entryName = entry.getName();
            if (entryName != null && entryName.equals(name))
            {
                selectedEntry = entry;
            }
        }
        return selectedEntry;
    }

    /**
     * Check if blkid can access the device.
     *
     * @param extCmd
     * @return
     * @throws StorageException
     */
    public static String blkid(@Nonnull ExtCmd extCmd, @Nonnull String device)
        throws StorageException
    {
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]{
                "blkid",
                device
            },
            "Failed execute blkid",
            "Failed to execute blkid",
            Commands.NO_RETRY
        );

        return new String(outputData.stdoutData, StandardCharsets.UTF_8);
    }

    public static List<LsBlkEntry> filterDeviceCandidates(List<LsBlkEntry> entries)
    {
        return entries.stream()
            .filter(lsBlkEntry -> lsBlkEntry.getParentName().isEmpty() &&
                lsBlkEntry.getSize() > MINIMAL_DEVICE_SIZE_BYTES &&
                lsBlkEntry.getMajor() != MAJOR_DRBD_NR)
            .filter(lsBlkEntry -> lsBlkEntry.getFsType().isEmpty())
            .filter(lsBlkEntry -> entries.stream()
                .noneMatch(entry -> entry.getParentName().equals(lsBlkEntry.getName())))
            .collect(Collectors.toList());
    }
}
