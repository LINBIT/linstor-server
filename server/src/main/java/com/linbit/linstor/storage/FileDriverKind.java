package com.linbit.linstor.storage;

import com.linbit.linstor.api.ApiConsts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FileDriverKind implements StorageDriverKind
{
    public FileDriverKind()
    {
    }

    @Override
    public String getDriverName()
    {
        return "FileDriver";
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

        keySet.add(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);

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
