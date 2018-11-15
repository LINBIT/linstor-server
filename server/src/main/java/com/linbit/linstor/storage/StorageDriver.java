package com.linbit.linstor.storage;

import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;

import java.util.Map;

/**
 * Handles allocation and deallocation of backing storage for DRBD devices
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorageDriver
{
    String STORAGE_NAMESPACE = InternalApiConsts.NAMESPC_INTERNAL + "/" + ApiConsts.NAMESPC_STORAGE_DRIVER;

    /**
     * Get the object representing this type of storage driver.
     *
     * @return The kind of this driver
     */
    StorageDriverKind getKind();

    /**
     * Makes a volume ready for access
     *
     * @param identifier Unique name of the volume
     * @param cryptKey the crypt key for the encrypted device (can be null if the device is / should not
     *  be encrypted)
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @throws StorageException If an operation on the storage fails
     */
    void startVolume(
        String identifier,
        String cryptKey,
        Props vlmDfnStorageProps
    )
        throws StorageException;

    /**
     * Shuts down a volume
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @throws StorageException If an operation on the storage fails
     */
    void stopVolume(
        String identifier,
        boolean isEncrypted,
        Props vlmDfnStorageProps
    )
        throws StorageException;

    /**
     * Creates a new volume
     *
     * @param identifier Unique name of the volume
     * @param size Size of the volume in KiB
     * @param cryptKey the crypt key for the encrypted device (can be null if the device is / should not
     *  be encrypted)
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @return Path of the volume's block device file
     * @throws StorageException If an operation on the storage fails
     */
    String createVolume(
        String identifier,
        long size,
        String cryptKey,
        Props vlmDfnStorageProps
    )
        throws StorageException, MaxSizeException, MinSizeException;

    /**
     * Resizes a volume
     *
     * @param identifier Unique name of the volume
     * @param size Size of the volume in KiB
     * @param cryptKey the crypt key for the encrypted device (can be null if the device is / should not
     *  be encrypted)
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @throws StorageException If an operation on the storage fails
     */
    void resizeVolume(
        String identifier,
        long size,
        String cryptKey,
        Props vlmDfnStorageProps
    )
        throws StorageException, MaxSizeException, MinSizeException;

    /**
     * Deletes a volume
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @throws StorageException If an operation on the storage fails
     */
    void deleteVolume(
        String identifier,
        boolean isEncrypted,
        Props vlmDfnStorageProps
    )
        throws StorageException;

    /**
     * Checks whether a volume exists.
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @return true if exists, otherwise false
     */
    boolean volumeExists(
        String identifier,
        boolean isEncrypted,
        Props vlmDfnStorageProps
    )
        throws StorageException;

    /**
     * Checks whether a volume exists and has the appropriate size
     *
     * @param identifier Unique name of the volume
     * @param requiredSize Expected size of the volume in kiB
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @throws StorageException If the check fails
     */
    SizeComparison compareVolumeSize(
        String identifier,
        long requiredSize,
        Props vlmDfnStorageProps
    )
        throws StorageException;

    /**
     * Returns the path of the volume's block device file
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @return Path of the volume's block device file
     * @throws StorageException If determining the path of the volume's
     *     block device files fails
     */
    String getVolumePath(
        String identifier,
        boolean isEncrypted,
        Props vlmDfnStorageProps
    )
        throws StorageException;

    /**
     * Returns the size of a volume
     *
     * @param identifier Unique name of the volume
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @return Size of the volume.
     * @throws StorageException If determining the size of the volume fails
     */
    long getSize(String identifier, Props vlmDfnStorageProps) throws StorageException;

    /**
     * Returns the total space of the pool.
     *
     * @return Size of storage pool.
     * @throws StorageException If getting the total space fails.
     */
    long getTotalSpace() throws StorageException;

    /**
     * Returns the free space in the pool.
     * This is the real capacity available to be used.
     * Defined as 'free_capacity' in
     * <a href=https://specs.openstack.org/openstack/cinder-specs/specs/kilo/over-subscription-in-thin-provisioning.html>
     * the OpenStack docs</a>.
     */
    long getFreeSpace() throws StorageException;

    /**
     * Returns a map of this driver's characteristics, such as what minimum unit of allocation
     * is for the storage managed by the driver, or whether the storage uses fat or thin
     * provisioning, etc.
     *
     * @return Map of key/value strings describing the driver's characteristics
     * @throws StorageException If determining the extent size of the volume group fails
     */
    Map<String, String> getTraits(String identifier) throws StorageException;

    /**
     * Sets the driver's configuration options
     *
     *
     * @param storPoolNameStr
     * @param storPoolNamespace Map of key/value strings to import into the driver's configuration
     * @param stltNamespace
     * @param nodeNamespace
     * @throws StorageException If the configuration contains options
     *     that are not valid for the driver
     */
    void setConfiguration(
        String storPoolNameStr,
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
        throws StorageException;

    /**
     * Creates a snapshot with the name of the {@code snapshotName} argument for the volume
     * {@code identifier} specifies.
     *
     * @param identifier
     * @param snapshotName
     * @throws StorageException
     * @throws UnsupportedOperationException if snapshots are not supported
     */
    void createSnapshot(String identifier, String snapshotName)
        throws StorageException;

    /**
     * Clones a given snapshot {@code identifier} which is the volume name and {@code snapshotName}
     * into a new volume ({@code targetIdentifier})
     *
     * @param snapshotName
     * @param targetIdentifier
     * @param cryptKey the crypt key for the encrypted device (can be null if the device is / should not
     *  be encrypted)
     * @param vlmDfnStorageProps a namespace in the {@link Volume}'s {@link PropsContainer} reserved for
     *  storage configuration. The {@link StorageDriver} is allowed to write to this namespace, and
     *  the entries of this namespace will be send back to the controller and persisted.
     *
     * @throws StorageException
     * @throws UnsupportedOperationException if snapshots are not supported
     */
    void restoreSnapshot(
        String sourceIdentifier,
        String snapshotName,
        String targetIdentifier,
        String cryptKey,
        Props vlmDfnStorageProps
    )
        throws StorageException;

    void rollbackVolume(
        String volumeIdentifier,
        String snapshotName,
        String cryptKey,
        Props vlmDfnProps
    )
        throws StorageException;

    /**
     * Deletes the given snapshot
     * @param volumeIdentifier
     * @param snapshotName
     * @throws StorageException
     * @throws UnsupportedOperationException if snapshots are not supported
     */
    void deleteSnapshot(String volumeIdentifier, String snapshotName)
        throws StorageException;

    /**
     * Checks whether a snapshot exists.
     *
     * @param volumeIdentifier Unique name of the volume
     * @param snapshotName
     * @return true if exists, otherwise false
     */
    boolean snapshotExists(String volumeIdentifier, String snapshotName) throws StorageException;

    /**
     * Returns a string representing the state of the volume. Defaults to null
     *
     * @param linstorVlmId
     * @return
     */
    default String getVolumeState(String linstorVlmId)
    {
        return null;
    }

    enum SizeComparison
    {
        TOO_SMALL,
        WITHIN_TOLERANCE,
        TOO_LARGE
    }

    enum VolumeType
    {
        VOLUME("volume"),
        SNAPSHOT("snapshot volume");

        private final String name;

        VolumeType(String nameRef)
        {
            name = nameRef;
        }

        public String getName()
        {
            return name;
        }
    }
}
