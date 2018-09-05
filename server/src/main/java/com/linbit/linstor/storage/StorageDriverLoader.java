package com.linbit.linstor.storage;

import java.util.Arrays;
import java.util.List;

public class StorageDriverLoader
{
    private static final List<StorageDriverKind> FACTORIES = Arrays.asList(
        new DisklessDriverKind(),
        new LvmDriverKind(),
        new LvmThinDriverKind(),
        new ZfsDriverKind(),
        new ZfsThinDriverKind(),
        new SwordfishTargetDriverKind(),
        new SwordfishInitiatorDriverKind()
    );

    public static StorageDriverKind getKind(String simpleName)
    {
        StorageDriverKind ret = null;
        for (StorageDriverKind factory : FACTORIES)
        {
            if (factory.getDriverName().equals(simpleName))
            {
                ret = factory;
                break;
            }
        }
        if (ret == null)
        {
            throw new IllegalArgumentException("Unknown storage driver " + simpleName);
        }
        return ret;
    }

    private StorageDriverLoader()
    {
    }
}
