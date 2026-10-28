package com.linbit.linstor.layer;

import com.linbit.linstor.storage.StorageException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A snapshot opened by {@link com.linbit.linstor.core.devmgr.DeviceHandler#openSnapshotReadOnly}. Closing it undoes
 * exactly what the open did, including the layers below, and resets
 * {@link com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject#getCloneDevicePath()}.
 */
@FunctionalInterface
public interface ReadOnlySnapshotHandle extends AutoCloseable
{
    @Override
    void close() throws StorageException;

    /**
     * Closes this handle after opening or closing a layer above it failed. A failure to close is added as suppressed
     * exception to the given failure, which the caller rethrows.
     */
    default void closeAfterFailure(Exception failure)
    {
        try
        {
            close();
        }
        catch (StorageException | RuntimeException exc)
        {
            failure.addSuppressed(exc);
        }
    }

    /**
     * Returns a handle that closes the given one only once, no matter how often it is closed.
     */
    static ReadOnlySnapshotHandle closeOnce(ReadOnlySnapshotHandle handle)
    {
        AtomicBoolean closed = new AtomicBoolean();
        return () ->
        {
            if (closed.compareAndSet(false, true))
            {
                handle.close();
            }
        };
    }
}
