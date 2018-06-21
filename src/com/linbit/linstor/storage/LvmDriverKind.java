package com.linbit.linstor.storage;

import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.timer.CoreTimer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LvmDriverKind implements StorageDriverKind
{
    @Override
    public String getDriverName()
    {
        return "LvmDriver";
    }

    @Override
    public StorageDriver makeStorageDriver(
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer,
        StltConfigAccessor stltCfgAccessor
    )
    {
        return new LvmDriver(errorReporter, fileSystemWatch, timer, this, stltCfgAccessor);
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
