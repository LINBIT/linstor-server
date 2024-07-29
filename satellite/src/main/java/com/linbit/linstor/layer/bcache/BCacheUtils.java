package com.linbit.linstor.layer.bcache;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BCacheUtils
{
    public static final Path PATH_SYS_FS_BCACHE = Paths.get("/", "sys", "fs", "bcache");
    private static final Path PATH_SYS_FS_BCACHE_REGISTER = PATH_SYS_FS_BCACHE.resolve("register");
    private static final Path PATH_SYS_BLOCK = Paths.get("/", "sys", "block");

    private static final Pattern PATTERN_MAKE_BCACHE_OUT_SET_UUID = Pattern.compile("^Set UUID:\\s+(.+)$");
    private static final Pattern PATTERN_BCACHE_SUPER_SHOW_OUT_CSET_UUID = Pattern.compile("^cset[.]uuid\\s+(.+)$");
    private static final Pattern PATTERN_BCACHE_SUPER_SHOW_OUT_BACKING_UUID = Pattern.compile("^dev[.]uuid\\s+(.+)$");
    private static final Pattern PATTERN_GREP_SYS_BLOCK_BCACHE = Pattern.compile(
        "^/sys/block/(bcache[0-9]+)/bcache/backing_dev_uuid:.*$" // last part is uuid
    );
    private static final int ATTACH_WAIT_SYS_FS_EXISTS_COUNT = 50;
    private static final long ATTACH_WAIT_SYS_FS_EXIST_TIMEOUT_IN_MS = 100;

    public static UUID makeBCache(
        ExtCmdFactory extCmdFactoryRef,
        @Nullable String backingDev,
        @Nullable String cacheDev,
        Collection<String> additionalArgs
    )
        throws StorageException
    {
        if (backingDev == null && cacheDev == null)
        {
            throw new ImplementationError("Either backing device or cache device has to be non-null!");
        }

        ArrayList<String> cmdList = new ArrayList<>();
        cmdList.add("make-bcache");
        if (backingDev != null)
        {
            cmdList.add("-B");
            cmdList.add(backingDev);
        }
        if (cacheDev != null)
        {
            cmdList.add("-C");
            cmdList.add(cacheDev);
        }
        cmdList.addAll(additionalArgs);

        String[] cmdArr = new String[cmdList.size()];
        cmdList.toArray(cmdArr);

        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create(),
            cmdArr,
            "Failed to make bcache device",
            "Failed to make bcache device"
        );

        return findUuid(outputData, PATTERN_MAKE_BCACHE_OUT_SET_UUID);
    }

    private static @Nullable UUID findUuid(OutputData outputData, Pattern pattern)
    {
        String out = new String(outputData.stdoutData);
        UUID ret = null;
        for (String line : out.trim().split(System.lineSeparator()))
        {
            Matcher mtc = pattern.matcher(line);
            if (mtc.find())
            {
                ret = UUID.fromString(mtc.group(1));
                break;
            }
        }
        return ret;
    }

    public static void remove(
        ErrorReporter errorReporterRef,
        UUID bcacheUuid,
        String bcacheIdentifier
    )
        throws StorageException
    {
        write(
            errorReporterRef,
            resolveFs(bcacheUuid, "unregister"),
            "1",
            "Failed to unregister bcache device " + bcacheIdentifier
        );
        write(
            errorReporterRef,
            resolveBlock(bcacheIdentifier, "stop"),
            "1",
            "Failed to stop bcache device " + bcacheIdentifier
        );
    }

    /**
     * @param bcacheUuid
     * @param relativePath
     *
     * @return Path.get("/sys/fs/bcache/" + bcacheUuid + "/" + relativePath)
     */
    private static Path resolveFs(UUID bcacheUuid, String relativePath)
    {
        return PATH_SYS_FS_BCACHE.resolve(bcacheUuid.toString()).resolve(relativePath);
    }

    /**
     * @param bcacheIdentifier
     * @param relativePath
     *
     * @return Path.get("/sys/block/" + bcacheIdentifier + "/bcache/" + relativePath)
     */
    private static Path resolveBlock(String bcacheIdentifier, String relativePath)
    {
        return PATH_SYS_BLOCK.resolve(bcacheIdentifier).resolve("bcache").resolve(relativePath);
    }

    public static void flush(ErrorReporter errorReporterRef, String bcacheIdentifierRef) throws StorageException
    {
        Path writebackPercentPath = resolveBlock(bcacheIdentifierRef, "writeback_percent");
        byte[] writebackPercent;
        try
        {
            errorReporterRef.logTrace("Querying writeback percent from %s", writebackPercentPath.toString());
            writebackPercent = Files.readAllBytes(writebackPercentPath);
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to query current writeback percent from " +
                    writebackPercentPath.toString(),
                exc
            );
        }

        long start = System.currentTimeMillis();
        write(
            errorReporterRef,
            writebackPercentPath,
            "0",
            "Failed to temporarily set writeback_percent to '0' for " + writebackPercentPath
        );
        errorReporterRef.logTrace("BCache: Force flush complete in %dms", (System.currentTimeMillis() - start));

        write(
            errorReporterRef,
            writebackPercentPath,
            new String(writebackPercent),
            "Failed to restore set writeback_percent for " + writebackPercentPath
        );
    }

    /**
     * @param extCmdFactoryRef
     * @param devicePath
     *
     * @return Returns the 'cset.uuid' from 'bcache-super-show $devicePath' or null
     *
     * @throws StorageException
     */
    public static @Nullable UUID getCSetUuidFromSuperBlock(ExtCmdFactory extCmdFactoryRef, String devicePath)
        throws StorageException
    {
        return getUuidFromBCacheSuperShow(extCmdFactoryRef, devicePath, PATTERN_BCACHE_SUPER_SHOW_OUT_CSET_UUID);
    }

    /**
     * @param extCmdFactoryRef
     * @param devicePath
     *
     * @return Returns the 'dev.uuid' from 'bcache-super-show $devicePath' or null
     *
     * @throws StorageException
     */
    public static @Nullable UUID getBackingId(ExtCmdFactory extCmdFactoryRef, String devicePath) throws StorageException
    {
        return getUuidFromBCacheSuperShow(extCmdFactoryRef, devicePath, PATTERN_BCACHE_SUPER_SHOW_OUT_BACKING_UUID);
    }

    private static @Nullable UUID getUuidFromBCacheSuperShow(
        ExtCmdFactory extCmdFactoryRef,
        String devicePath,
        Pattern pattern
    )
        throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmdFactoryRef.create(),
            new String[] {
                "bcache-super-show",
                devicePath
            },
            "Failed to read bcache super block of " + devicePath,
            "Failed to read bcache super block of " + devicePath,
            Commands.SKIP_EXIT_CODE_CHECK
        );
        UUID uuid = null;
        if (outputData.exitCode == 0)
        {
            uuid = findUuid(outputData, pattern);
        }
        return uuid;
    }


    public static void register(ErrorReporter errorReporterRef, String devicePathRef)
        throws StorageException
    {
        write(
            errorReporterRef,
            PATH_SYS_FS_BCACHE_REGISTER,
            devicePathRef,
            "Failed to register bcache device " + devicePathRef
        );
    }

    public static void attach(ErrorReporter errorReporterRef, UUID cacheUuid, String bcacheIdentifier)
        throws StorageException
    {
        Path attachPath = resolveBlock(bcacheIdentifier, "attach");
        for (int wait = 0; !Files.exists(attachPath) && wait < ATTACH_WAIT_SYS_FS_EXISTS_COUNT; wait++)
        {
            try
            {
                Thread.sleep(ATTACH_WAIT_SYS_FS_EXIST_TIMEOUT_IN_MS);
            }
            catch (InterruptedException ignored)
            {
            }
        }
        if (!Files.exists(attachPath))
        {
            throw new StorageException(
                "Path does not exist after waiting " +
                    ATTACH_WAIT_SYS_FS_EXIST_TIMEOUT_IN_MS * ATTACH_WAIT_SYS_FS_EXISTS_COUNT + "ms: " + attachPath
            );
        }

        write(
            errorReporterRef,
            attachPath,
            cacheUuid.toString(),
            "Failed to attach cache device " + cacheUuid + " to bcache " + bcacheIdentifier
        );
    }

    private static void write(ErrorReporter errorReporterRef, Path path, String content, String failMsg)
        throws StorageException
    {
        try
        {
            errorReporterRef.logTrace("echo %s > %s", content, path.toString());
            Files.write(path, content.getBytes());
        }
        catch (IOException exc)
        {
            throw new StorageException(failMsg, exc);
        }
    }

    /**
     * Executes a
     *
     * <pre>
     * grep $uuid /sys/block/bcache* /bcache/backing_dev_uuid
     * </pre>
     *
     * and returns the "bcache*" part or null if not found
     *
     * @param extCmdFactory
     * @param backingUuidRef
     *
     * @return
     *
     * @throws StorageException
     */
    public static @Nullable String getIdentifierByBackingUuid(ExtCmdFactory extCmdFactory, UUID backingUuidRef)
        throws StorageException
    {
        OutputData outputData = Commands.genericExecutor(
            extCmdFactory.create(),
            new String[] {
                "/bin/bash",
                "-c",
                "grep -H --color=never " + backingUuidRef.toString() +
                    " /sys/block/bcache*/bcache/backing_dev_uuid"
            },
            "Failed to find bcache device for backing dev uuid " + backingUuidRef,
            "Failed to find bcache device for backing dev uuid " + backingUuidRef,
            Commands.SKIP_EXIT_CODE_CHECK
        );
        String ret = null;
        if (outputData.exitCode == 0)
        {
            Matcher matcher = PATTERN_GREP_SYS_BLOCK_BCACHE.matcher(new String(outputData.stdoutData));
            if (!matcher.find())
            {
                throw new StorageException("Found path does not match pattern: " + new String(outputData.stdoutData));
            }
            ret = matcher.group(1);
        }
        return ret;
    }
}
