package com.linbit.linstor.compat;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.List;

public class CompatibilityUtils
{
    private static final int BIT_DRBD = 1;
    private static final int BIT_NVME = 2;

    private CompatibilityUtils()
    {
    }

    public static Resource.Flags mapDisklessFlagToNvmeOrDrbd(List<DeviceLayerKind> layerList)
    {
        Resource.Flags ret;

        boolean hasDrdbKind = layerList.contains(DeviceLayerKind.DRBD);
        boolean hasNvmeKind = layerList.contains(DeviceLayerKind.NVME);

        int kindCase = hasDrdbKind ? BIT_DRBD : 0;
        kindCase |= hasNvmeKind ? BIT_NVME : 0;
        switch (kindCase)
        {
            case 0:
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_STACK,
                        "The given storage pool is a diskless storage pool, but no layer " +
                            "from the given layer-list supports diskless storage pools"
                    )
                );
            case BIT_DRBD:
                ret = Resource.Flags.DRBD_DISKLESS;
                break;
            case BIT_NVME:
                ret = Resource.Flags.NVME_INITIATOR;
                break;
            case BIT_DRBD | BIT_NVME:
                int nvmeIdx = layerList.indexOf(DeviceLayerKind.NVME);
                if (layerList.indexOf(DeviceLayerKind.DRBD) < nvmeIdx)
                {
                    ret = Resource.Flags.NVME_INITIATOR;
                }
                else
                {
                    ret = Resource.Flags.DRBD_DISKLESS;
                }
                break;
            default:
                throw new ImplementationError("not implemented");
        }
        return ret;
    }
}
