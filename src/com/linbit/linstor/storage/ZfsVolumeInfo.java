package com.linbit.linstor.storage;

import java.io.File;
import java.io.IOException;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;

public class ZfsVolumeInfo extends VolumeInfo
{

    public ZfsVolumeInfo(long size, String identifier, String path)
    {
        super(size, identifier, path);
    }

    public static ZfsVolumeInfo getInfo(
        final ExtCmd ec,
        final String zfsCommand,
        final String pool,
        final String identifier
    )
        throws ChildProcessTimeoutException, IOException, StorageException
    {
        // TODO call the command
        // zfs list -o used -Hp linstorpool/identifier

        final String[] command = getZfsVolumeInfoCommand(zfsCommand, pool, identifier);

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

        String stringSize = new String(outputData.stdoutData);
        if (stringSize.contains("\n"))
        {
            stringSize = stringSize.substring(0, stringSize.indexOf('\n'));
        }
        long size = Long.parseLong(stringSize);
        size >>= 10; // driver wants the count in KiB...
        final String path = File.separator + "dev" +
            File.separator + "zvol" +
            File.separator + pool +
            File.separator + identifier;
        return new ZfsVolumeInfo(size, identifier, path);
    }

    public static String[] getZfsVolumeInfoCommand(final String zfsCommand, final String pool, final String identifier)
    {
        return new String[]
        {
            zfsCommand,
            "list",
            "-H", // no headers
            "-p", // parsable version, tab spaced, in bytes
            "-o", "volsize", // print specified columns only
            pool + File.separator + identifier // the specified volume
        };
    }

}
