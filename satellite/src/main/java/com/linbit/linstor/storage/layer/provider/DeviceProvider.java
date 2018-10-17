package com.linbit.linstor.storage.layer.provider;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer;

import java.util.List;
import java.util.Map;

public interface DeviceProvider
{
    Map<Volume, StorageException> adjust(List<Volume> volumes)
        throws StorageException;

    /**
     * @return the capacity of the used storage pool.
     * @throws StorageException
     */
    long getPoolCapacity(StorPool storPool) throws StorageException;

    /**
     * @return the free space of the used storage pool.
     * @throws StorageException
     */
    long getPoolFreeSpace(StorPool storPool) throws StorageException;

    /**
     * Creates a snapshot of a given {@link Volume}.
     *
     * @param vlm
     * @param snapshotName
     * @throws StorageException
     */
    void createSnapshot(Volume vlm, String snapshotName) throws StorageException;

    /**
     * Restores a snapshot from the {@link Volume} of the first parameter into the
     * {@link Volume} of the last parameter.
     *
     * @param srcVlm
     * @param snapshotName
     * @param targetVlm
     * @throws StorageException
     */
    void restoreSnapshot(Volume srcVlm, String snapshotName, Volume targetVlm) throws StorageException;

    /**
     * Deletes a given snapshot.
     * @param vlm
     * @param snapshotName
     * @throws StorageException
     */
    void deleteSnapshot(Volume vlm, String snapshotName) throws StorageException;

    /**
     * @param vlm
     * @param snapshotName
     * @return true if the given {@link Volume} has a snapshot with the given name.
     * @throws StorageException
     */
    boolean snapshotExists(Volume vlm, String snapshotName) throws StorageException;

    /**
     * Checks if the given {@link StorPool} has a valid configuration for all involved {@link DeviceLayer}s.
     *
     * @param config
     * @throws StorageException
     */
    void checkConfig(StorPool storPool) throws StorageException;
}
