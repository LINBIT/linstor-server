package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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

    public static List<LsBlkEntry> lsblk(ExtCmd extCmd)
        throws StorageException
    {
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd.setSaveWithoutSharedLocks(true),
            new String[]{
                "lsblk",
                "-P",
                "-b",
                "--paths",
                "-o", Arrays.stream(LsBlkEntry.LsBlkFields.values())
                    .map(LsBlkEntry.LsBlkFields::toString)
                    .collect(Collectors.joining(","))
            },
            "Failed execute lsblk",
            "Failed to execute lsblk"
        );

        return parseLsblkOutput(new String(outputData.stdoutData, StandardCharsets.UTF_8));
    }

    public static boolean parentIsVDO(ExtCmd extCmd, @Nonnull List<String> devicePaths)
        throws StorageException
    {
        if (devicePaths.isEmpty()) {
            return false;
        }

        List<LsBlkEntry> entries = lsblk(extCmd);
        for (final String devicePath : devicePaths) {
            Optional<LsBlkEntry> dev = entries.stream()
                .filter(entry -> entry.getName() != null && entry.getName().equals(devicePath))
                .findFirst();
            if (dev.isPresent()) {
                Optional<LsBlkEntry> parent = entries.stream()
                    .filter(entry -> entry.getName() != null && entry.getName().equals(dev.get().getParentName()))
                    .findFirst();
                if (parent.isPresent()) {
                    if (!parent.get().getFsType().equalsIgnoreCase("vdo")) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public static String[] blkid(ExtCmd extCmd)
        throws StorageException
    {
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]{
                "blkid",
                "-o", "device"
            },
            "Failed execute blkid",
            "Failed to execute blkid",
            Commands.NO_RETRY,
            Collections.singletonList(2)
        );

        final String blkIdResult = new String(outputData.stdoutData, StandardCharsets.UTF_8);
        return blkIdResult.split("\n");
    }

    public static List<LsBlkEntry> filterDeviceCandidates(List<LsBlkEntry> entries, final String[] blkIdEntries)
    {
        final List<String> blkIds = Arrays.asList(blkIdEntries);
        return entries.stream()
            .filter(lsBlkEntry -> lsBlkEntry.getParentName().isEmpty() &&
                lsBlkEntry.getSize() > MINIMAL_DEVICE_SIZE_BYTES &&
                lsBlkEntry.getMajor() != MAJOR_DRBD_NR)
            .filter(lsBlkEntry -> entries.stream()
                .noneMatch(entry -> entry.getParentName().equals(lsBlkEntry.getName())))
            .filter(lsBlkEntry -> blkIds.stream().noneMatch(blkid -> blkid.endsWith(lsBlkEntry.getName())))
            .collect(Collectors.toList());
    }
}
