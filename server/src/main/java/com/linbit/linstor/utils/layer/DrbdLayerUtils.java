package com.linbit.linstor.utils.layer;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Set;

public class DrbdLayerUtils
{

    private DrbdLayerUtils()
    {
    }

    public static boolean isAnyDrbdResourceExpected(AccessContext accCtx, Resource rscRef)
        throws AccessDeniedException
    {
        boolean ret = false;
        Set<AbsRscLayerObject<Resource>> drbdRscSet = LayerRscUtils
            .getRscDataByProvider(rscRef.getLayerData(accCtx), DeviceLayerKind.DRBD);
        for (AbsRscLayerObject<Resource> drbdRsc : drbdRscSet)
        {
            if (isDrbdResourceExpected(accCtx, (DrbdRscData<Resource>) drbdRsc))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public static boolean isDrbdResourceExpected(AccessContext accCtx, DrbdRscData<Resource> rscData)
        throws AccessDeniedException
    {
        boolean isDevExpected = true;

        StateFlags<Flags> rscFlags = rscData.getAbsResource().getStateFlags();
        if (rscFlags.isSet(accCtx, Resource.Flags.DRBD_DISKLESS))
        {
            isDevExpected = true;
        }
        else
        {
            boolean hasNvmeBelow = !LayerRscUtils.getRscDataByProvider(rscData, DeviceLayerKind.NVME).isEmpty();
            boolean isNvmeTarget = !rscFlags.isSet(accCtx, Resource.Flags.NVME_INITIATOR);
            boolean isEbsTarget = false;
            for (AbsRscLayerObject<Resource> storRscData : LayerRscUtils.getRscDataByProvider(
                rscData,
                DeviceLayerKind.STORAGE
            ))
            {
                for (VlmProviderObject<Resource> vlmData : storRscData.getVlmLayerObjects().values())
                {
                    if (vlmData.getProviderKind().equals(DeviceProviderKind.EBS_TARGET))
                    {
                        isEbsTarget = true;
                        break;
                    }
                }
                if (isEbsTarget)
                {
                    break;
                }
            }
            boolean isInactive = rscFlags.isSet(accCtx, Resource.Flags.INACTIVE);
            if (hasNvmeBelow && isNvmeTarget || isEbsTarget || isInactive)
            {
                // target NVME or inactive resource will never return a device, so drbd will not exist
                isDevExpected = false;
            }
        }

        return isDevExpected;
    }

    public static boolean isDrbdDevicePresent(DrbdRscData<Resource> rscData)
    {
        return rscData.streamVlmLayerObjects().allMatch(
            vlmData -> vlmData.exists() && vlmData.getDevicePath() != null
        );
    }

    public static boolean isForceInitialSyncSet(AccessContext accCtx, DrbdRscData<Resource> drbdRscData)
        throws InvalidKeyException,
        AccessDeniedException
    {
        String forceSync = drbdRscData.getAbsResource()
            .getResourceDefinition()
            .getProps(accCtx)
            .getProp(InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA, ApiConsts.NAMESPC_DRBD_OPTIONS);
        return forceSync != null && Boolean.parseBoolean(forceSync);
    }


}
