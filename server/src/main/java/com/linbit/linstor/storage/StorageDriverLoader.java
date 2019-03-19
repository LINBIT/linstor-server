package com.linbit.linstor.storage;

import com.linbit.ImplementationError;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.RemoveAfterDevMgrRework;

import java.util.Arrays;
import java.util.List;

public class StorageDriverLoader
{
    @RemoveAfterDevMgrRework
    private static final List<StorageDriverKind> FACTORIES = Arrays.asList(
        new DisklessDriverKind(),
        new LvmDriverKind(),
        new LvmThinDriverKind(),
        new ZfsDriverKind(),
        new ZfsThinDriverKind(),
        new SwordfishTargetDriverKind(),
        new SwordfishInitiatorDriverKind()
    );

    @RemoveAfterDevMgrRework // instead use a mapping to DeviceLayerKind
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

    @RemoveAfterDevMgrRework
    public static DeviceProviderKind getDeviceProviderKind(StorageDriverKind oldKind)
    {
        DeviceProviderKind newKind;
        if (oldKind instanceof DisklessDriverKind)
        {
            newKind = DeviceProviderKind.DISKLESS;
        }
        else if (oldKind instanceof LvmDriverKind)
        {
            newKind = DeviceProviderKind.LVM;
        }
        else if (oldKind instanceof LvmThinDriverKind)
        {
            newKind = DeviceProviderKind.LVM_THIN;
        }
        else if (oldKind instanceof ZfsDriverKind)
        {
            newKind = DeviceProviderKind.ZFS;
        }
        else if (oldKind instanceof ZfsThinDriverKind)
        {
            newKind = DeviceProviderKind.ZFS_THIN;
        }
        else if (oldKind instanceof SwordfishTargetDriverKind)
        {
            newKind = DeviceProviderKind.SWORDFISH_TARGET;
        }
        else if (oldKind instanceof SwordfishInitiatorDriverKind)
        {
            newKind = DeviceProviderKind.SWORDFISH_INITIATOR;
        }
        else
        {
            throw new ImplementationError("Unknown storage driver kind: " +
                oldKind.getDriverName() + " " + oldKind.getClass().getSimpleName());
        }
        return newKind;
    }

    private StorageDriverLoader()
    {
    }
}
