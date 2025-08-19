package com.linbit.linstor.utils.layer;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.VolumeUtils;

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
            .getRscDataByLayer(rscRef.getLayerData(accCtx), DeviceLayerKind.DRBD);
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
            boolean hasNvmeBelow = !LayerRscUtils.getRscDataByLayer(rscData, DeviceLayerKind.NVME).isEmpty();
            boolean isNvmeTarget = !rscFlags.isSet(accCtx, Resource.Flags.NVME_INITIATOR);
            boolean isEbsTarget = false;
            for (AbsRscLayerObject<Resource> storRscData : LayerRscUtils.getRscDataByLayer(
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

    public static boolean skipInitSync(AccessContext accCtxRef, DrbdVlmData<Resource> drbdVlmDataRef)
        throws AccessDeniedException
    {
        boolean skipInitSync;
        if (DrbdLayerUtils.isForceInitialSyncSet(accCtxRef, drbdVlmDataRef.getRscLayerObject()))
        {
            skipInitSync = false;
        }
        else
        {
            skipInitSync = VolumeUtils.isVolumeThinlyBacked(drbdVlmDataRef, true);

            if (!skipInitSync)
            {
                skipInitSync = VolumeUtils.getStorageDevices(
                    drbdVlmDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA)
                )
                    .stream()
                    .map(VlmProviderObject::getProviderKind)
                    .allMatch(kind -> kind == DeviceProviderKind.ZFS || kind == DeviceProviderKind.ZFS_THIN);
            }
        }
        return skipInitSync;
    }
}
