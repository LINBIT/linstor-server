package com.linbit.linstor.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LvmThinDriverKind implements StorageDriverKind
{
    @Override
    public String getDriverName()
    {
        return "LvmThinDriver";
    }

    @Override
    public LvmThinDriver makeStorageDriver()
    {
        return new LvmThinDriver(this);
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        final HashMap<String, String> traits = new HashMap<>();

        traits.put(DriverTraits.KEY_PROV, DriverTraits.PROV_THIN);

        return traits;
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        final HashSet<String> keySet = new HashSet<>();

        keySet.add(StorageConstants.CONFIG_LVM_CREATE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_REMOVE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_CHANGE_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY);
        keySet.add(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_THIN_POOL_KEY);

        return keySet;
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return true;
    }
}
