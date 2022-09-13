package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LvmThinDriverKind implements StorageDriverKind
{
    private static final String VG_PREFIX = "linstor_";

    public static String VGName(String poolName)
    {
        String vgName;
        int sepIndex = poolName.indexOf('/');
        if (sepIndex == -1)
        {
            vgName = VG_PREFIX + poolName;
        }
        else
        {
            vgName = poolName.substring(0, sepIndex);
        }
        return vgName;
    }

    public static String LVName(String poolName)
    {
        return poolName.substring(poolName.indexOf('/') + 1);
    }

    @Override
    public String getDriverName()
    {
        return "LvmThinDriver";
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        final HashMap<String, String> traits = new HashMap<>();

        traits.put(ApiConsts.KEY_STOR_POOL_PROVISIONING, ApiConsts.VAL_STOR_POOL_PROVISIONING_THIN);

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
        keySet.add(StorageConstants.CONFIG_LVM_CONVERT_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_LVS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_VGS_COMMAND_KEY);
        keySet.add(StorageConstants.CONFIG_SIZE_ALIGN_TOLERANCE_KEY);
        keySet.add(StorageConstants.CONFIG_LVM_THIN_POOL_KEY);

        return keySet;
    }

    @Override
    public boolean isSnapshotSupported()
    {
        return true;
    }

    @Override
    public boolean hasBackingStorage()
    {
        return true;
    }
}
