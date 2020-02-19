package com.linbit.linstor.storage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OpenflexTargetDriverKind implements StorageDriverKind
{
    @Override
    public String getDriverName()
    {
        return "SwordfishTargetDriver";
    }

    @Override
    public Map<String, String> getStaticTraits()
    {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> getConfigurationKeys()
    {
        final HashSet<String> keySet = new HashSet<>();

        // keySet.add(StorageConstants.CONFIG_OF_URL_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_COMPOSED_DEVICE_NAME_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_STOR_POOL_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_POLL_TIMEOUT_VLM_CRT_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_POLL_RETRIES_VLM_CRT_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_POLL_TIMEOUT_ATTACH_VLM_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_POLL_RETRIES_ATTACH_VLM_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_POLL_TIMEOUT_GREP_NVME_UUID_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_POLL_RETRIES_GREP_NVME_UUID_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_STOR_SVC_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_USER_NAME_KEY);
        // keySet.add(StorageConstants.CONFIG_OF_USER_PW_KEY);

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

    @Override
    public boolean supportsDrbd()
    {
        return false;
    }
}
