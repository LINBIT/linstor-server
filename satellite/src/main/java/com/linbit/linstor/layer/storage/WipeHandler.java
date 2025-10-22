package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.Platform;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.layer.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.utils.ExceptionThrowingConsumer;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.Collections;

@Singleton
public class WipeHandler
{
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;

    @Inject
    public WipeHandler(
        ExtCmdFactory extCmdFactoryRef,
        ErrorReporter errorReporterRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        errorReporter = errorReporterRef;
    }

    /**
     * Only wipes linstor-known data.
     *
     * That means, this method calls "{@code wipefs devicePath}" and cleans drbd super block (last 4k of the device)
     *
     * @param devicePath
     *
     * @throws StorageException
     * @throws IOException
     */
    public void quickWipe(String devicePath) throws StorageException
    {
        Commands.wipeFs(extCmdFactory.create(), Collections.singleton(devicePath));
        try
        {
            MdSuperblockBuffer.wipe(extCmdFactory, devicePath, true);
            MdSuperblockBuffer.wipe(extCmdFactory, devicePath, false);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("Failed to quick-wipe devicePath " + devicePath, ioExc);
        }
    }

    public void asyncWipe(String devicePath, ExceptionThrowingConsumer<String, StorageException> wipeFinishedNotifier)
        throws StorageException
    {
        // TODO: this step should be asynchron

        /*
         * for security reasons we should wipe (zero out) an lvm / zfs before actually removing it.
         *
         * however, user may want to skip this step for performance reasons.
         * in that case, we still need to make sure to at least wipe DRBD's signature so that
         * re-allocating the same storage does not find the data-garbage from last DRBD configuration
         */
        try
        {
            MdSuperblockBuffer.wipe(extCmdFactory, devicePath, true);
            MdSuperblockBuffer.wipe(extCmdFactory, devicePath, false);
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            // wipe failed, but we still need to free the allocated space
        }
        finally
        {
            wipeFinishedNotifier.accept(devicePath);
        }
    }

    public OutputData wipeFs(String device) throws StorageException
    {
        return wipeFs(extCmdFactory, device);
    }

    public static OutputData wipeFs(ExtCmdFactory extCmdFactoryRef, String device) throws StorageException
    {
        OutputData res = null;

        if (Platform.isLinux())
        {
            res = Commands.genericExecutor(
                extCmdFactoryRef.create(),
                new String[]
                {
                    "wipefs",
                    "-a",
                    device
                },
                "Failed to clear BCache metadata",
                "Failed to clear BCache metadata"
            );
        }
        else if (Platform.isWindows())
        {
            /* TODO: should this do something? */
            res = new OutputData(
                    new String[] {},
                    new byte[] {},
                    new byte[] {},
                    0
            );
        }
        else
        {
            throw new ImplementationError("Platform is neither Linux nor Windows, please add support for it to LINSTOR");
        }
        return res;
    }
}
