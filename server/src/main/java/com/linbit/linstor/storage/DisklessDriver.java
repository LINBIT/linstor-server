package com.linbit.linstor.storage;

import com.linbit.linstor.propscon.Props;

import java.util.Collections;
import java.util.Map;

public class DisklessDriver implements StorageDriver
{
    private final StorageDriverKind storageDriverKind;

    public DisklessDriver(StorageDriverKind storageDriverKindRef)
    {
        storageDriverKind = storageDriverKindRef;
    }

    @Override
    public StorageDriverKind getKind()
    {
        return storageDriverKind;
    }

    @Override
    public void startVolume(String identifier, String cryptKey, Props vlmStorageProps)
    {
        // no-op
    }

    @Override
    public void stopVolume(String identifier, boolean isEncrypted, Props vlmStorageProps)
    {
        // no-op
    }

    @Override
    public String createVolume(String identifier, long size, String cryptKey, Props vlmStorageProps)
    {
        return "none";
    }

    @Override
    public void resizeVolume(String identifier, long size, String cryptKey, Props vlmStorageProps)
        throws StorageException
    {
        // no-op
    }

    @Override
    public boolean volumeExists(String identifier, boolean isEncrypted, Props vlmStorageProps)
    {
        return true;
    }

    @Override
    public void deleteVolume(String identifier, boolean isEncrypted, Props vlmStorageProps)
    {
        // no-op
    }

    @Override
    public SizeComparison compareVolumeSize(String identifier, long requiredSize, Props vlmStorageProps)
    {
        return SizeComparison.WITHIN_TOLERANCE;
    }

    @Override
    public String getVolumePath(String identifier, boolean isEncrypted, Props vlmStorageProps)
    {
        return "none";
    }

    @Override
    public long getSize(String identifier, Props vlmStorageProps)
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Map<String, String> getTraits(final String identifier)
    {
        return Collections.emptyMap();
    }

    @Override
    public void setConfiguration(
        String storPoolNameStr,
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
        throws StorageException
    {
        // no-op
    }

    @Override
    public void createSnapshot(String identifier, String snapshotName)
    {
        // no-op
    }

    @Override
    public void restoreSnapshot(
        String sourceIdentifier,
        String snapshotName,
        String targetIdentifier,
        String cryptKey,
        Props vlmStorageProps
    )
    {
        // no-op
    }

    @Override
    public void deleteSnapshot(String volumeIdentifier, String snapshotName)
    {
        // no-op
    }

    @Override
    public boolean snapshotExists(String volumeIdentifier, String snapshotName)
        throws StorageException
    {
        return true;
    }

    @Override
    public long getTotalSpace() {
        return Long.MAX_VALUE;
    }

    @Override
    public long getFreeSpace()
    {
        return Long.MAX_VALUE;
    }
}
