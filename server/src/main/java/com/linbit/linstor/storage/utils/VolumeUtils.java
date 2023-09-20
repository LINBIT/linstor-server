package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.HashSet;
import java.util.Set;

public class VolumeUtils
{
    /**
     * If {@code partial} is true, this method returns true if at least one of the backing devices
     * is thinly backed.
     * If {@code partial} is false, this method only returns true if all backing devices are
     * thinly backed.
     *
     * @param vlmObj
     * @param partial
     * @return
     */
    public static boolean isVolumeThinlyBacked(VlmProviderObject<?> vlmObj, boolean partial)
    {
        Set<VlmProviderObject<?>> backingVlms = getStorageDevices(vlmObj);

        boolean ret;
        if (partial)
        {
            ret = false;
            for (VlmProviderObject<?> tmpVlmObj : backingVlms)
            {
                if (tmpVlmObj.getProviderKind().usesThinProvisioning())
                {
                    ret = true;
                    break;
                }
            }
        }
        else
        {
            ret = true;
            for (VlmProviderObject<?> tmpVlmObj : backingVlms)
            {
                if (!tmpVlmObj.getProviderKind().usesThinProvisioning())
                {
                    ret = false;
                    break;
                }
            }
        }
        return ret;
    }

    /**
     * Check if the volume is provided on ZFS.
     *
     * @param vlmObj
     * @param partial if true just any of the storage devices needs to be zfs, otherwise all.
     * @return
     */
    public static boolean isVolumeZfs(VlmProviderObject<?> vlmObj, boolean partial)
    {
        Set<VlmProviderObject<?>> backingVlms = getStorageDevices(vlmObj);

        boolean ret;
        if (partial)
        {
            ret = false;
            for (VlmProviderObject<?> tmpVlmObj : backingVlms)
            {
                if (tmpVlmObj.getProviderKind() == DeviceProviderKind.ZFS ||
                    tmpVlmObj.getProviderKind() == DeviceProviderKind.ZFS_THIN)
                {
                    ret = true;
                    break;
                }
            }
        }
        else
        {
            ret = true;
            for (VlmProviderObject<?> tmpVlmObj : backingVlms)
            {
                if (!(tmpVlmObj.getProviderKind() == DeviceProviderKind.ZFS ||
                    tmpVlmObj.getProviderKind() == DeviceProviderKind.ZFS_THIN))
                {
                    ret = false;
                    break;
                }
            }
        }
        return ret;
    }

    public static Set<VlmProviderObject<?>> getStorageDevices(VlmProviderObject<?> vlmObj)
    {
        Set<VlmProviderObject<?>> backingVlms = new HashSet<>();
        Set<VlmProviderObject<?>> toExpand = new HashSet<>();
        if (vlmObj instanceof VlmLayerObject)
        {
            toExpand.add(vlmObj);
        }
        else
        {
            backingVlms.add(vlmObj);
        }

        while (!toExpand.isEmpty())
        {
            Set<VlmProviderObject<?>> toExpandNext = new HashSet<>();
            for (VlmProviderObject<?> tmpVlmObj : toExpand)
            {
                Set<?> rscChildren = tmpVlmObj.getRscLayerObject().getChildren();
                for (Object rscChildObj : rscChildren)
                {
                    AbsRscLayerObject<?> rscChild = (AbsRscLayerObject<?>) rscChildObj;
                    VlmProviderObject<?> childVlmObj = rscChild.getVlmProviderObject(tmpVlmObj.getVlmNr());
                    if (childVlmObj != null)
                    {
                        if (childVlmObj instanceof VlmLayerObject)
                        {
                            toExpandNext.add(childVlmObj);
                        }
                        else
                        {
                            backingVlms.add(childVlmObj);
                        }
                    }
                }

            }
            toExpand = toExpandNext;
        }
        return backingVlms;
    }

    private VolumeUtils()
    {
    }
}
