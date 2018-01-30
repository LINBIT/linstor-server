package com.linbit.linstor.storage;

import java.util.Arrays;
import java.util.List;

public class StorageDriverLoader
{
    private static List<StorageDriverKind> FACTORIES = Arrays.asList(
        new DisklessDriverKind(),
        new LvmDriverKind(),
        new LvmThinDriverKind(),
        new ZfsDriverKind()
    );

    public static StorageDriverKind getKind(String simpleName)
    {
        for (StorageDriverKind factory : FACTORIES)
        {
            if (factory.getDriverName().equals(simpleName))
            {
                return factory;
            }
        }

        throw new IllegalArgumentException("Unknown storage driver " + simpleName);
    }

    private StorageDriverLoader()
    {
    }
}
