package com.linbit.linstor.storage;

import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.utils.CryptSetup;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.timer.CoreTimer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SwordfishDriverKind implements StorageDriverKind
{
    @Override
    public String getDriverName()
    {
        return "SwordfishDriver";
    }
    @Override
    public StorageDriver makeStorageDriver(
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer,
        StltConfigAccessor stltCfgAccessor
    )
    {
        return new SwordfishDriver(
            errorReporter,
            this,
            new RestHttpClient(),
            timer,
            new CryptSetup(timer, errorReporter)
        );
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

        keySet.add(StorageConstants.CONFIG_SWORDFISH_HOST_PORT_KEY);
        keySet.add(StorageConstants.CONFIG_SWORDFISH_COMPOSED_NODE_NAME_KEY);
        keySet.add(StorageConstants.CONFIG_SWORDFISH_STOR_POOL_KEY);
        keySet.add(StorageConstants.CONFIG_LINSTOR_STOR_POOL_KEY);
        keySet.add(StorageConstants.CONFIG_SWORDFISH_POLL_TIMEOUT_KEY);
        keySet.add(StorageConstants.CONFIG_SWORDFISH_STOR_SVC_KEY);
        keySet.add(StorageConstants.CONFIG_SWORDFISH_USER_NAME_KEY);
        keySet.add(StorageConstants.CONFIG_SWORDFISH_USER_PW_KEY);

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