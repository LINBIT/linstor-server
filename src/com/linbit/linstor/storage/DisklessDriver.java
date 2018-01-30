package com.linbit.linstor.storage;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.linbit.linstor.SatelliteCoreServices;

public class DisklessDriver implements StorageDriver
{
    @Override
    public void initialize(SatelliteCoreServices coreSvc)
    {
        // no-op
    }

    @Override
    public void startVolume(String identifier)
    {
        // no-op
    }

    @Override
    public void stopVolume(String identifier)
    {
        // no-op
    }

    @Override
    public String createVolume(String identifier, long size)
    {
        return "none";
    }

    @Override
    public void deleteVolume(String identifier)
    {
        // no-op
    }

    @Override
    public void checkVolume(String identifier, long size)
    {
        // no-op
    }

    @Override
    public String getVolumePath(String identifier)
    {
        return "none";
    }

    @Override
    public long getSize(String identifier)
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Map<String, String> getTraits()
    {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        return Collections.emptySet();
    }

    @Override
    public void setConfiguration(Map<String, String> config)
    {
        // no-op
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return false; // tempting... :)
    }

    @Override
    public void createSnapshot(String identifier, String snapshotName)
    {
        // no-op
    }

    @Override
    public void restoreSnapshot(String sourceIdentifier, String snapshotName, String targetIdentifier)
    {
        // no-op
    }

    @Override
    public void deleteSnapshot(String identifier, String snapshotName)
    {
        // no-op
    }

    @Override
    public long getFreeSize() {
        return 0;
    }
}
