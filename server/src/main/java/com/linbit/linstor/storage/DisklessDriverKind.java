package com.linbit.linstor.storage;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DisklessDriverKind implements StorageDriverKind
{
    @Override
    public String getDriverName()
    {
        return "DisklessDriver";
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        return Collections.emptySet();
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return false;
    }

    @Override
    public boolean hasBackingStorage()
    {
        return false;
    }

    @Override
    public boolean needsConfiguration()
    {
        return false;
    }
}
