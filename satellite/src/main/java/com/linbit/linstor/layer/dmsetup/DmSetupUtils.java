package com.linbit.linstor.layer.dmsetup;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.layer.storage.utils.Commands;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.StringUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DmSetupUtils
{
    private static final String DM_SETUP_MESSAGE_FLUSH = "flush";
    private static final String DM_SETUP_MESSAGE_FLUSH_ON_SUSPEND = "flush_on_suspend";

    private static final Pattern DM_SETUP_LS_PATTERN = Pattern.compile(
        "^([^\\s]+)\\s+\\(([0-9]+)(?::\\s|,\\s)([0-9]+)\\)$",
        Pattern.MULTILINE
    );

    private DmSetupUtils()
    {
    }

    public static Set<String> list(ExtCmd extCmd, String target) throws StorageException
    {
        Set<String> ret = new HashSet<>();
        try
        {
            OutputData outputData;
            if (target == null)
            {
                outputData = extCmd.exec("dmsetup", "ls");
            }
            else
            {
                outputData = extCmd.exec("dmsetup", "ls", "--target", target);
            }
            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "listing devices from dmsetup ls failed "
            );

            String stdOut = new String(outputData.stdoutData);
            Matcher matcher = DM_SETUP_LS_PATTERN.matcher(stdOut);
            while (matcher.find())
            {
                String devName = matcher.group(1);
                // String major = matcher.group(2);
                // String minor = matcher.group(3);

                ret.add(devName);
            }
        }
        catch (IOException ioExc)
        {
            throw new StorageException(
                "Failed to list writecache devices",
                ioExc
            );
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Listing writecache devices timed out",
                exc
            );
        }
        return ret;
    }

    public static void remove(ExtCmd extCmd, String identifier) throws StorageException
    {
        Commands.genericExecutor(
            extCmd,
            new String[]
            {
                "dmsetup", "remove", "--retry", identifier
            },
            "Failed to remove writecache device",
            "Failed to remove writecache device"
        );
    }

    public static void createWritecache(
        ExtCmdFactory extCmdFactory,
        String identifierRef,
        String dataDevice,
        String cacheDevice,
        boolean isCachePmem,
        long blockSize,
        String writecacheArgs
    )
        throws StorageException
    {
        long startSector = 0;
        long endSector = Commands.getDeviceSizeInSectors(extCmdFactory.create(), dataDevice);

        Commands.genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
            // * dmsetup create dm_log --table "0 1562758832 writecache p /dev/sdb /dev/pmem0 4096 4 high_watermark 10
                "dmsetup",
                "create",
                identifierRef,
                "--table",
                // all following arguments have to be in one String-argument, not single array elements!
                StringUtils.join(
                    " ",
                    startSector,
                    endSector,
                    "writecache",
                    isCachePmem ? "p" : "s",
                    dataDevice,
                    cacheDevice,
                    blockSize,
                    writecacheArgs == null ? "" : writecacheArgs
                )
            },
            "Failed to create writecache device",
            "Failed to create writecache device"
        );
    }

    public static void createCache(
        ExtCmdFactory extCmdFactory,
        String identifierRef,
        String dataDevice,
        String cacheDevice,
        String metaDevice,
        long blockSize,
        String feature,
        String policy,
        String policyArgs
    )
        throws StorageException
    {
        long startSector = 0;
        long endSector = Commands.getDeviceSizeInSectors(extCmdFactory.create(), dataDevice);

        Commands.genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                "dmsetup",
                "create",
                identifierRef,
                "--table",
                // all following arguments have to be in one String-argument, not single array elements!
                StringUtils.join(
                    " ",
                    startSector,
                    endSector,
                    "cache",
                    metaDevice,
                    cacheDevice,
                    dataDevice,
                    blockSize,
                    "1", // 1 feature - not sure if there are more possible...
                    feature,
                    policy,
                    policyArgs
                )
            },
            "Failed to create cache device",
            "Failed to create cache device"
        );
    }

    public static boolean isSuspended(
        ExtCmd extCmd,
        String device
    )
        throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmd,
            new String[]
                {
                    "dmsetup",
                    "info",
                    "-C",
                    "-o", "suspended",
                    "--noheadings",
                    device
                },
            "'dmsetup suspend' returned unexpected exit code",
            "Failed to suspend device " + device
        );

        String stdOut = new String(outputData.stdoutData);
        return stdOut.trim().equalsIgnoreCase("suspended");
    }

    public static void suspend(
        ExtCmd extCmd,
        String device
    )
        throws StorageException
    {
        Commands.genericExecutor(
            extCmd,
            new String[]
                {
                    "dmsetup",
                    "suspend",
                    device
                },
            "'dmsetup suspend' returned unexpected exit code",
            "Failed to suspend device " + device
        );
    }

    public static void resume(
        ExtCmd extCmd,
        String device
    )
        throws StorageException
    {
        Commands.genericExecutor(
            extCmd,
            new String[]
                {
                    "dmsetup",
                    "resume",
                    device
                },
            "'dmsetup resume' returned unexpected exit code",
            "Failed to resume device " + device
        );
    }

    public static void flush(
        ExtCmdFactory extCmdFactory,
        String device
    )
        throws StorageException
    {
        message(extCmdFactory, device, 0L, DM_SETUP_MESSAGE_FLUSH);
    }

    public static void flushOnSuspend(
        ExtCmdFactory extCmdFactory,
        String device
    )
        throws StorageException
    {
        message(extCmdFactory, device, 0L, DM_SETUP_MESSAGE_FLUSH_ON_SUSPEND);
    }

    public static void message(
        ExtCmdFactory extCmdFactory,
        String device,
        Long sector,
        String message
    )
        throws StorageException
    {
        String sectorStr = sector == null ? "0" : sector.toString();
        Commands.genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                "dmsetup",
                "message",
                device,
                sectorStr,
                message
            },
            "'dmsetup message' returned unexpected exit code",
            "Failed to send message '" + message + "' to device " + device + " (sector: " + sectorStr + ")"
        );
    }
}
