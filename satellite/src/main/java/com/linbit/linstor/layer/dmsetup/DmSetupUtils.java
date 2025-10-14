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
    private static final String[] DM_SETUP_MESSAGE_FLUSH = new String[] {
        "flush"
    };
    private static final String[] DM_SETUP_MESSAGE_FLUSH_ON_SUSPEND = new String[] {
        "flush_on_suspend"
    };

    private static final Pattern DM_SETUP_LS_PATTERN = Pattern.compile(
        "^([^\\s]+)\\s+\\(([0-9]+)[:,]\\s*([0-9]+)\\)$",
        Pattern.MULTILINE
    );

    private static final String DM_SETUP_TYPE_CACHE = "cache";
    private static final int DM_SETUP_STATUS_CACHE_TYPE_IDX = 2;
    private static final int DM_SETUP_STATUS_CACHE_DIRTY_BLOCKS_IDX = 13;

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

    public static long getLastEventNr(ExtCmdFactory extCmdFactory, VlmProviderObject<Resource> vlmDataRef)
        throws ChildProcessTimeoutException, IOException, ExtCmdFailedException
    {
        long ret;
        String[] cmd = {
            "dmsetup",
            "info",
            "-c", // columns
            "-o", "events", // show only event number column
            "--noheadings", // make an educated guess
            vlmDataRef.getDevicePath()
        };
        try
        {
            OutputData outputData = extCmdFactory.create().exec(cmd);
            if (outputData.exitCode != 0)
            {
                throw new ExtCmdFailedException(cmd, outputData);
            }
            ret = Long.parseLong(new String(outputData.stdoutData).trim());
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new ExtCmdFailedException(cmd, timeoutExc);
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(cmd, ioExc);
        }
        return ret;
    }
    public static Set<String> list(ExtCmd extCmd, @Nullable String target) throws StorageException
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
        DmSetupUtils.create(
            extCmdFactory,
            identifierRef,
            getWriteCacheTable(
                dataDevice,
                cacheDevice,
                isCachePmem,
                blockSize,
                writecacheArgs,
                0,
                Commands.getDeviceSizeInSectors(extCmdFactory.create(), dataDevice)
            )
        );
    }

    public static void create(
        ExtCmdFactory extCmdFactory,
        String identifierRef,
        String dmsetupTableRef
    )
        throws StorageException
    {
        Commands.genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                "dmsetup",
                "create",
                identifierRef,
                "--table",
                dmsetupTableRef
            },
            "Failed to create dmsetup device, identifier: " + identifierRef + ", used table: " + dmsetupTableRef,
            "Failed to create dmsetup device, identifier: " + identifierRef + ", used table: " + dmsetupTableRef
        );
    }

    /**
     * <p>Generates a "table" for dmsetup as follows:<br/>
     * <code>'{startSector} {endSector} writecache {"p" if isCachePmem else "s"} {dataDevice} {cacheDevice} {blockSize}
     * {writecacheArgs}'</code></p>
     * <p>Example:<br />
     * <code>'0 1562758832 writecache p /dev/sdb /dev/pmem0 4096 2 high_watermark 10'</code></p>
     */
    public static String getWriteCacheTable(
        String dataDevice,
        String cacheDevice,
        boolean isCachePmem,
        long blockSize,
        String writecacheArgs,
        long startSector,
        long endSector
    )
    {
        return StringUtils.join(
            " ",
            startSector,
            endSector,
            "writecache",
            isCachePmem ? "p" : "s",
            dataDevice,
            cacheDevice,
            blockSize,
            writecacheArgs == null ? "" : writecacheArgs
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
        DmSetupUtils.create(
            extCmdFactory,
            identifierRef,
            getCacheTable(
                0L,
                Commands.getDeviceSizeInSectors(extCmdFactory.create(), dataDevice),
                dataDevice,
                cacheDevice,
                metaDevice,
                blockSize,
                feature,
                policy,
                policyArgs
            )
        );
    }

    public static void reload(
        ExtCmdFactory extCmdFactoryRef,
        String identifierRef,
        String dmsetupTableRef
    )
        throws StorageException
    {
        Commands.genericExecutor(
            extCmdFactoryRef.create(),
            new String[]
            {
                "dmsetup",
                "reload",
                identifierRef,
                "--table",
                dmsetupTableRef
            },
            "Failed to reload dmsetup device, identifier: " + identifierRef + ", used table: " + dmsetupTableRef,
            "Failed to reload dmsetup device, identifier: " + identifierRef + ", used table: " + dmsetupTableRef
        );
    }

    /**
     * <p>Generates a "table" for dmsetup as follows:<br/>
     * <code>'{startSector} {endSector} cache {metaDevice} {cacheDevice} {dataDevice} {blockSize}
     * 1 {feature} {policy} {policyArgs}'</code></p>
     * <p>Example:<br />
     * <code>'0 2105344 cache /dev/scratch/rsc.dmcache_meta_00000 /dev/scratch/rsc.dmcache_cache_00000
     * /dev/scratch/rsc_00000 4096 1 writeback smq 0 '</code></p>
     */
    public static String getCacheTable(
        long startSector,
        long endSector,
        String dataDevice,
        String cacheDevice,
        String metaDevice,
        long blockSize,
        @Nullable String feature,
        @Nullable String policy,
        String... policyArgs
    )
    {
        return StringUtils.join(
            " ",
            startSector,
            endSector,
            "cache",
            metaDevice,
            cacheDevice,
            dataDevice,
            blockSize,
            feature == null ? "0" : "1 " + feature,
            policy == null ? "" : policy,
            policyArgs == null || policyArgs.length == 0 ?
                "0" :
                policyArgs.length + " " + StringUtils.join(" ", policyArgs)
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
        String[] message
    )
        throws StorageException
    {
        String sectorStr = sector == null ? "0" : sector.toString();
        Commands.genericExecutor(
            extCmdFactory.create(),
            StringUtils.concat(new String[]
            {
                "dmsetup",
                "message",
                device,
                sectorStr
            }, message),
            "'dmsetup message' returned unexpected exit code",
            "Failed to send message '" + message + "' to device " + device + " (sector: " + sectorStr + ")"
        );
    }

    /**
     * This method only works with "cache" types. NOT with "writecache" or any other dmsetup type!
     */
    public static long getDirtyCacheBlocks(
        ExtCmdFactory extCmdFactory,
        String device
    )
        throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmdFactory.create(),
            new String[]
            {
                "dmsetup", "status", device
            },
            "'dmsetup status " + device + "' failed with unexpected exit code",
            "'dmsetup status " + device + "' failed"
        );
        String stdOut = new String(outputData.stdoutData).trim();
        String[] parts = stdOut.split(" ");
        // safety-check:
        if (!DM_SETUP_TYPE_CACHE.equals(parts[2]))
        {
            throw new StorageException(
                "Expected '" + DM_SETUP_TYPE_CACHE + "' but found: '" + parts[DM_SETUP_STATUS_CACHE_TYPE_IDX] +
                    "'. Full status: " + stdOut
            );
        }
        return Long.parseLong(parts[DM_SETUP_STATUS_CACHE_DIRTY_BLOCKS_IDX]);
    }


    public static void wait(ExtCmd extCmd, String device, @Nullable Long lastEventNrRef)
        throws StorageException, ChildProcessTimeoutException, ExtCmdFailedException
    {
        String[] cmd = lastEventNrRef == null ?
            new String[]
            {
                "dmsetup", "wait", device
            } :
            new String[]
            {
                "dmsetup", "wait", device, Long.toString(lastEventNrRef)
            };
        OutputData outputData;
        try
        {
            outputData = extCmd.exec(cmd);
            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "'%s' failed with unexpected exit code",
                StringUtils.join(" ", cmd)
            );
        }
        catch (IOException ioExc)
        {
            throw new ExtCmdFailedException(cmd, ioExc);
        }
    }
}
