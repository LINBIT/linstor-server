package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LvmDriverKind implements StorageDriverKind
{
    public LvmDriverKind()
    {
    }

    @Override
    public String getDriverName()
    {
        return "LvmDriver";
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
        final HashSet<String> keySet = new HashSet<>();

        keySet.add(StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_RESIZE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        keySet.add(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);

        return keySet;
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
