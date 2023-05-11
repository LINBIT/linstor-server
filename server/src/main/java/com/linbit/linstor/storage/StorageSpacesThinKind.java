package com.linbit.linstor.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StorageSpacesThinKind implements StorageDriverKind
{
    public StorageSpacesThinKind()
    {
    }

    @Override
    public String getDriverName()
    {
        return "StorageSpacesThinDriver";
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        return new HashMap<>();
     }

    @Override
    public Set<String> getConfigurationKeys()
    {
        return new HashSet<>();
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return false;
    }

    @Override
    public boolean hasBackingStorage()
    {
        return true;
    }
}
