package com.linbit.linstor.layer.dmsetup;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.utils.ExceptionThrowingConsumer;
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
        "^([^\\s]+)\\s+\\(([0-9]+)[:,]\\s*([0-9]+)\\)$",
        Pattern.MULTILINE
    );

    private DmSetupUtils()
    {
    }

    public static void suspendIo(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AbsRscLayerObject<Resource> rscData,
        boolean suspendRef,
        @Nullable ExceptionThrowingConsumer<VlmProviderObject<Resource>, StorageException> preExecConsumerRef
    )
        throws StorageException, ExtCmdFailedException
    {
        for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
        {
            suspendIo(errorReporter, extCmdFactory, vlmData, suspendRef, preExecConsumerRef);
        }
    }

    public static void suspendIo(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        VlmProviderObject<Resource> vlmData,
        boolean suspendRef,
        @Nullable ExceptionThrowingConsumer<VlmProviderObject<Resource>, StorageException> preExecConsumerRef
    )
        throws StorageException, ExtCmdFailedException
    {
        if (suspendRef && preExecConsumerRef != null)
        {
            preExecConsumerRef.accept(vlmData);
        }

        errorReporter.logTrace(
            "%s IO for %s resource '%s', volume: %d",
            suspendRef ? "Suspending" : "Resuming",
            vlmData.getLayerKind().name(),
            vlmData.getRscLayerObject().getSuffixedResourceName(),
            vlmData.getVlmNr().value
        );
        String[] cmd = {
            "dmsetup",
            suspendRef ? "suspend" : "resume",
            vlmData.getDevicePath()
        };
        try
        {
            extCmdFactory.create().exec(cmd);
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new ExtCmdFailedException(cmd, timeoutExc);
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(cmd, ioExc);
        }
    }

    public static boolean isSuspended(ExtCmdFactory extCmdFactoryRef, AbsRscLayerObject<Resource> rscData)
        throws ExtCmdFailedException
    {
        boolean isAnyVlmSuspended = false;
        for (VlmProviderObject<Resource> vlmData : rscData.getVlmLayerObjects().values())
        {
            if (isSuspended(extCmdFactoryRef, vlmData))
            {
                isAnyVlmSuspended = true;
                break;
            }
        }
        return isAnyVlmSuspended;
    }

    public static boolean isSuspended(ExtCmdFactory extCmdFactoryRef, VlmProviderObject<Resource> vlmDataRef)
        throws ExtCmdFailedException
    {
        boolean isSuspended;
        String[] cmd = {
            "dmsetup",
            "info",
            "-c", // columns
            "-o", "attr", // show only attributes
            "--noheadings", // make an educated guess
            vlmDataRef.getDevicePath()
        };
        try
        {
            OutputData outputData = extCmdFactoryRef.create().exec(cmd);
            if (outputData.exitCode != 0)
            {
                throw new ExtCmdFailedException(cmd, outputData);
            }
            isSuspended = new String(outputData.stdoutData).contains("s"); // "s" in "attr" column means "suspended"
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new ExtCmdFailedException(cmd, timeoutExc);
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(cmd, ioExc);
        }
        return isSuspended;
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
        AbsRscLayerObject<Resource> rscDataRef
    )
        throws StorageException
    {
        for (VlmProviderObject<Resource> vlmData : rscDataRef.getVlmLayerObjects().values())
        {
            flush(extCmdFactory, vlmData);
        }
    }

    public static void flush(
        ExtCmdFactory extCmdFactory,
        VlmProviderObject<Resource> vlmDataRef
    )
        throws StorageException
    {
        message(extCmdFactory, vlmDataRef.getDevicePath(), 0L, DM_SETUP_MESSAGE_FLUSH);
    }

    public static void flushOnSuspend(
        ExtCmdFactory extCmdFactory,
        VlmProviderObject<Resource> vlmDataRef
    )
        throws StorageException
    {
        message(extCmdFactory, vlmDataRef.getDevicePath(), 0L, DM_SETUP_MESSAGE_FLUSH_ON_SUSPEND);
    }

    public static void message(
        ExtCmdFactory extCmdFactory,
        String device,
        @Nullable Long sector,
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
