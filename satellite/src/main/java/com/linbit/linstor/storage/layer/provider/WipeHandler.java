package com.linbit.linstor.storage.layer.provider;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.MdSuperblockBuffer;
import com.linbit.linstor.storage.layer.provider.utils.Commands;

import java.io.IOException;
import java.util.function.Consumer;

public class WipeHandler
{
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;

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
     * @param extCmd
     * @param devicePath
     *
     * @throws StorageException
     * @throws IOException
     */
    public void quickWipe(String devicePath) throws StorageException
    {
        Commands.wipeFs(extCmdFactory.create(), devicePath);
        try
        {
            MdSuperblockBuffer.wipe(devicePath);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("Failed to quick-wipe devicePath " + devicePath, ioExc);
        }
    }

    public void asyncWipe(String devicePath, Consumer<String> wipeFinishedNotifier)
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
            MdSuperblockBuffer.wipe(devicePath);
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
}
