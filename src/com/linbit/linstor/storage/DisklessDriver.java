package com.linbit.linstor.storage;

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
    public void startVolume(String identifier, String cryptKey)
    {
        // no-op
    }

    @Override
    public void stopVolume(String identifier, boolean isEncrypted)
    {
        // no-op
    }

    @Override
    public String createVolume(String identifier, long size, String cryptKey)
    {
        return "none";
    }

    @Override
    public void resizeVolume(String identifier, long size, String cryptKey)
        throws StorageException
    {
        // no-op
    }

    @Override
    public boolean volumeExists(String identifier, boolean isEncrypted, VolumeType volumeType)
    {
        return true;
    }

    @Override
    public void deleteVolume(String identifier, boolean isEncrypted)
    {
        // no-op
    }

    @Override
    public SizeComparison compareVolumeSize(String identifier, long requiredSize)
    {
        return SizeComparison.WITHIN_TOLERANCE;
    }

    @Override
    public String getVolumePath(String identifier, boolean isEncrypted)
    {
        return "none";
    }

    @Override
    public long getSize(String identifier)
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Map<String, String> getTraits(final String identifier)
    {
        return Collections.emptyMap();
    }

    @Override
    public void setConfiguration(Map<String, String> config)
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
        String cryptKey
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
    public long getFreeSpace()
    {
        return Long.MAX_VALUE;
    }
}
