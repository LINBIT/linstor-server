package com.linbit.linstor.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.provider.utils.Commands;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LsBlkUtils
{
    private static final long MINIMAL_DEVICE_SIZE_BYTES = 1024 * 1024 * 1024;

    private static final int MAJOR_DRBD_NR = 147;

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
            lsBlkEntries.add(new LsBlkEntry(fields));
        }
        return lsBlkEntries;
    }

    public static List<LsBlkEntry> lsblk(ExtCmd extCmd)
        throws StorageException
    {
        ExtCmd.OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]{
                "lsblk",
                "-P",
                "-b",
                "-o", Arrays.stream(LsBlkEntry.LsBlkFields.values())
                    .map(LsBlkEntry.LsBlkFields::toString)
                    .collect(Collectors.joining(","))
            },
            "Failed execute lsblk",
            "Failed to execute lsblk"
        );

        return parseLsblkOutput(new String(outputData.stdoutData, StandardCharsets.UTF_8));
    }

    public static List<LsBlkEntry> filterDeviceCandidates(List<LsBlkEntry> entries)
    {
        return entries.stream()
            .filter(lsBlkEntry -> lsBlkEntry.getParentName().isEmpty() &&
                lsBlkEntry.getSize() > MINIMAL_DEVICE_SIZE_BYTES &&
                lsBlkEntry.getMajor() != MAJOR_DRBD_NR)
            .filter(lsBlkEntry -> entries.stream()
                .noneMatch(entry -> entry.getParentName().equals(lsBlkEntry.getName())))
            .collect(Collectors.toList());
    }
}
