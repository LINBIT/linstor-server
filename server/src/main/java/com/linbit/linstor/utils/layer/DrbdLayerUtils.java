package com.linbit.linstor.utils.layer;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.VolumeUtils;

import java.util.Set;

public class DrbdLayerUtils
{

    private DrbdLayerUtils()
    {
    }

    public static boolean isAnyDrbdResourceExpected(Resource rscRef)
    {
        boolean ret = false;
        Set<AbsRscLayerObject<Resource>> drbdRscSet = LayerRscUtils
            .getRscDataByLayer(rscRef.getLayerData(), DeviceLayerKind.DRBD);
        for (AbsRscLayerObject<Resource> drbdRsc : drbdRscSet)
        {
            if (isDrbdResourceExpected((DrbdRscData<Resource>) drbdRsc))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    public static boolean isDrbdResourceExpected(DrbdRscData<Resource> rscData)
    {
        boolean isDevExpected = true;

        StateFlags<Flags> rscFlags = rscData.getAbsResource().getStateFlags();
        if (rscFlags.isSet(Resource.Flags.DRBD_DISKLESS))
        {
            isDevExpected = true;
        }
        else
        {
            boolean hasNvmeBelow = !LayerRscUtils.getRscDataByLayer(rscData, DeviceLayerKind.NVME).isEmpty();
            boolean isNvmeTarget = !rscFlags.isSet(Resource.Flags.NVME_INITIATOR);
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
            boolean isInactive = rscFlags.isSet(Resource.Flags.INACTIVE);
            if ((hasNvmeBelow && isNvmeTarget) || isEbsTarget || isInactive)
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

    public static boolean isForceInitialSyncSet(DrbdRscData<Resource> drbdRscData)
        throws InvalidKeyException
    {
        return isForceInitialSyncSet(drbdRscData.getAbsResource().getResourceDefinition());
    }

    public static boolean isForceInitialSyncSet(ResourceDefinition rscDfn)
        throws InvalidKeyException
    {
        @Nullable String forceSync = rscDfn.getProps()
            .getProp(InternalApiConsts.KEY_FORCE_INITIAL_SYNC_PERMA, ApiConsts.NAMESPC_DRBD_OPTIONS);
        return forceSync != null && Boolean.parseBoolean(forceSync);
    }

    public static boolean skipInitSync(DrbdVlmData<Resource> drbdVlmDataRef)
    {
        boolean skipInitSync;
        boolean allEbs = VolumeUtils.getStorageDevices(
            drbdVlmDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA)
        )
            .stream()
            .map(VlmProviderObject::getProviderKind)
            .allMatch(
                kind -> kind == DeviceProviderKind.EBS_INIT || kind == DeviceProviderKind.EBS_TARGET
            );
        if (allEbs)
        {
            /*
             * Like thin volumes, freshly created EBS volumes are guaranteed to read as zeros, so all
             * replicas start out identical and the initial sync can be skipped. For EBS the skip is
             * also mandatory, even if an initial sync is forced (e.g. by the mixed-storage-pool
             * detection): the initial-UpToDate node is an EBS target, which never creates DRBD
             * meta-data locally, so no sync source can ever exist and an EBS initiator waiting for
             * one would wait forever.
             */
            skipInitSync = true;
        }
        else if (DrbdLayerUtils.isForceInitialSyncSet(drbdVlmDataRef.getRscLayerObject()))
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

    public static boolean isTiebreaker(Resource rscRef)
    {
        StateFlags<Flags> flags = rscRef.getStateFlags();
        return flags.isSet(Resource.Flags.TIE_BREAKER);
    }

    public static boolean setTiebreaker(Resource tiebreakerRef, boolean enableRef)
        throws DatabaseException
    {
        StateFlags<Flags> flags = tiebreakerRef.getStateFlags();
        boolean changed = enableRef != flags.isSet(Resource.Flags.TIE_BREAKER);
        if (changed)
        {
            if (enableRef)
            {
                flags.enableFlags(Resource.Flags.TIE_BREAKER);
            }
            else
            {
                flags.disableFlags(Resource.Flags.TIE_BREAKER);
                flags.enableFlags(Resource.Flags.DRBD_DISKLESS);
            }
        }
        return changed;
    }

    public static boolean setClientFlag(Resource rscRef, boolean enableRef)
        throws DatabaseException
    {
        boolean changed;
        AbsRscLayerObject<Resource> rscData = rscRef.getLayerData();
        if (rscData instanceof DrbdRscData<Resource> drbdRscData)
        {
            StateFlags<DrbdRscObject.DrbdRscFlags> flags = drbdRscData.getFlags();
            changed = enableRef != flags.isSet(DrbdRscObject.DrbdRscFlags.CLIENT);
            if (changed)
            {
                if (enableRef)
                {
                    flags.enableFlags(DrbdRscObject.DrbdRscFlags.CLIENT);
                }
                else
                {
                    flags.disableFlags(DrbdRscObject.DrbdRscFlags.CLIENT);
                }
            }
        }
        else
        {
            changed = false;
        }
        return changed;
    }

    public static boolean isDrbdClient(Resource rscRef)
    {
        boolean ret = false;
        AbsRscLayerObject<Resource> rscData = rscRef.getLayerData();
        if (rscData instanceof DrbdRscData<Resource> drbdRscData)
        {
            StateFlags<DrbdRscObject.DrbdRscFlags> flags = drbdRscData.getFlags();
            ret = flags.isSet(DrbdRscObject.DrbdRscFlags.CLIENT);
        }
        return ret;
    }
}
