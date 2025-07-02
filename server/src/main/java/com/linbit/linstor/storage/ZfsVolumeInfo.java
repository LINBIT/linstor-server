package com.linbit.linstor.storage;

import java.io.File;
import java.io.IOException;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.utils.ShellUtils;

public class ZfsVolumeInfo extends VolumeInfo
{
    private final long used;

    public ZfsVolumeInfo(long size, long usedRef, String identifier, String path)
    {
        super(size, identifier, path);
        used = usedRef;
    }

    public long getUsed()
    {
        return used;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static ZfsVolumeInfo getInfo(
        final ExtCmd ec,
        final String zfsCommand,
        final String pool,
        final String identifier
    )
        throws StorageException
    {
        ZfsVolumeInfo volumeInfo;
        final String[] command = getZfsVolumeInfoCommand(zfsCommand, pool, identifier);
        try
        {
            OutputData outputData = ec.exec(command);
            if (outputData.exitCode != 0)
            {
                StringBuilder commandBuilder = new StringBuilder();
                for (String commandPart : command)
                {
                    commandBuilder.append(commandPart).append(" ");
                }
                throw new StorageException(
                    String.format("Command returned with exitCode %d and message %s: %s",
                        outputData.exitCode,
                        new String(outputData.stderrData),
                        commandBuilder.toString()));
            }

            String rawOut = new String(outputData.stdoutData);
            if (rawOut.contains("\n"))
            {
                rawOut = rawOut.substring(0, rawOut.indexOf('\n'));
            }
            String[] parts = rawOut.split("\t");
            if (parts.length < 2)
            {
                throw new StorageException(
                    "ZFS listing output has unexpected number of entries",
                    "Pool: " + pool + ", zvol: " + identifier + "; output to parse: '" + rawOut + "'",
                    null,
                    null,
                    "External command used: " + ShellUtils.joinShellQuote(command)
                );
            }
            long size = Long.parseLong(parts[0]) >> 10; // convert to  KiB
            long used = Long.parseLong(parts[1]) >> 10; // convert to  KiB
            final String path = File.separator + "dev" +
                File.separator + "zvol" +
                File.separator + pool +
                File.separator + identifier;
            volumeInfo = new ZfsVolumeInfo(size, used, identifier, path);
        }
        catch (ChildProcessTimeoutException | IOException exc)
        {
            throw new StorageException(
                "Failed to get volume information",
                String.format("Failed to get information for volume: %s", identifier),
                (exc instanceof ChildProcessTimeoutException) ?
                    "External command timed out" :
                    "External command threw an IOException",
                null,
                String.format(
                    "External command: %s",
                    ShellUtils.joinShellQuote(command)
                ),
                exc
            );
        }
        return volumeInfo;
    }

    public static String[] getZfsVolumeInfoCommand(final String zfsCommand, final String pool, final String identifier)
    {
        return new String[]
        {
            zfsCommand,
            "list",
            "-H", // no headers
            "-p", // parsable version, tab spaced, in bytes
            "-o", "volsize,used", // print specified columns only
            pool + File.separator + identifier // the specified volume
        };
    }

}
