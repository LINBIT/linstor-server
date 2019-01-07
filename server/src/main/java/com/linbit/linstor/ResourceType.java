package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.storage.layer.kinds.CryptSetupLayerKind;
import com.linbit.linstor.storage.layer.kinds.DefaultLayerKind;
import com.linbit.linstor.storage.layer.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.layer.kinds.DrbdLayerKind;
import com.linbit.linstor.storage.layer.kinds.StorageLayerKind;

public enum ResourceType
{
    DEFAULT(new DefaultLayerKind()), // special type, only temporary used until the rework is completed

    DRBD(new DrbdLayerKind()),
    CRYPT(new CryptSetupLayerKind()),

    STORAGE(new StorageLayerKind());
//    LVM(new LvmLayerKind()),
//    LVM_THIN(new LvmThinLayerKind()),
//    ZFS(new ZfsLayerKind()),
//    ZFS_THIN(new ZfsThinLayerKind());

    private final DeviceLayerKind kind;

    ResourceType(DeviceLayerKind kindRef)
    {
        kind = kindRef;
    }

    public DeviceLayerKind getDevLayerKind()
    {
        if (kind == null)
        {
            throw new ImplementationError("Invalid deviceLayerKind requested from type: " + this.name());
        }
        return kind;
    }
}
