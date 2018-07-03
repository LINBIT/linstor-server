package com.linbit.linstor.storage;

import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import java.util.Map;

/**
 * Handles allocation and deallocation of backing storage for DRBD devices
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorageDriver
{
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
     *
     * @throws StorageException If an operation on the storage fails
     */
    void startVolume(String identifier, String cryptKey) throws StorageException;

    /**
     * Shuts down a volume
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     *
     * @throws StorageException If an operation on the storage fails
     */
    void stopVolume(String identifier, boolean isEncrypted) throws StorageException;

    /**
     * Creates a new volume
     *
     * @param identifier Unique name of the volume
     * @param size Size of the volume in KiB
     * @param cryptKey the crypt key for the encrypted device (can be null if the device is / should not
     *  be encrypted)
     *
     * @return Path of the volume's block device file
     * @throws StorageException If an operation on the storage fails
     */
    String createVolume(String identifier, long size, String cryptKey)
        throws StorageException, MaxSizeException, MinSizeException;

    /**
     * Resizes a volume
     *
     * @param identifier Unique name of the volume
     * @param size Size of the volume in KiB
     * @param cryptKey the crypt key for the encrypted device (can be null if the device is / should not
     *  be encrypted)
     *
     * @throws StorageException If an operation on the storage fails
     */
    void resizeVolume(String identifier, long size, String cryptKey)
        throws StorageException, MaxSizeException, MinSizeException;

    /**
     * Deletes a volume
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     *
     * @throws StorageException If an operation on the storage fails
     */
    void deleteVolume(String identifier, boolean isEncrypted) throws StorageException;

    /**
     * Checks whether a volume exists.
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     * @param volumeType
     * @return true if exists, otherwise false
     */
    boolean volumeExists(String identifier, boolean isEncrypted, VolumeType volumeType) throws StorageException;

    /**
     * Checks whether a volume exists and has the appropriate size
     *
     * @param identifier Unique name of the volume
     * @param requiredSize Expected size of the volume in kiB
     * @throws StorageException If the check fails
     */
    SizeComparison compareVolumeSize(String identifier, long requiredSize) throws StorageException;

    /**
     * Returns the path of the volume's block device file
     *
     * @param identifier Unique name of the volume
     * @param isEncrypted
     *
     * @return Path of the volume's block device file
     * @throws StorageException If determining the path of the volume's
     *     block device files fails
     */
    String getVolumePath(String identifier, boolean isEncrypted) throws StorageException;

    /**
     * Returns the size of a volume
     *
     * @param identifier Unique name of the volume
     * @return Size of the volume.
     * @throws StorageException If determining the size of the volume fails
     */
    long getSize(String identifier) throws StorageException;

    /**
     * Returns the free space in the pool. null if the pool does not have a clearly defined free space (e.g. thinly
     * provisioned).
     */
    Long getFreeSpace() throws StorageException;

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
     * @param config Map of key/value strings to import into the driver's configuration
     * @throws StorageException If the configuration contains options
     *     that are not valid for the driver
     */
    void setConfiguration(Map<String, String> config) throws StorageException;

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
     *
     * @throws StorageException
     * @throws UnsupportedOperationException if snapshots are not supported
     */
    void restoreSnapshot(
        String sourceIdentifier,
        String snapshotName,
        String targetIdentifier,
        String cryptKey
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
