package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;
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
    public Map<String, String> getStaticTraits()
    {
        final HashMap<String, String> traits = new HashMap<>();

        traits.put(ApiConsts.KEY_STOR_POOL_PROVISIONING, ApiConsts.VAL_STOR_POOL_PROVISIONING_FAT);

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

    @Override
    public boolean isSnapshotDependent()
    {
        return true;
    }

    @Override
    public boolean hasBackingStorage()
    {
        return true;
    }
}
