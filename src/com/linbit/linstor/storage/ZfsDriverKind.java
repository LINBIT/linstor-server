package com.linbit.linstor.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ZfsDriverKind implements StorageDriverKind
{
    @Override
    public String getDriverName()
    {
        return "ZfsDriver";
    }

    @Override
    public ZfsDriver makeStorageDriver()
    {
        return new ZfsDriver(this);
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        final HashMap<String, String> traits = new HashMap<>();

        traits.put(DriverTraits.KEY_PROV, DriverTraits.PROV_FAT);

        return traits;
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        HashSet<String> keys = new HashSet<>();
        keys.add(StorageConstants.CONFIG_ZFS_POOL_KEY);
        keys.add(StorageConstants.CONFIG_ZFS_COMMAND_KEY);
        keys.add(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);

        //TODO zfs offers a lot of editable properties, for example recordsize..

        return keys;
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return true;
    }
}
