package com.linbit.linstor.utils.layer;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class LayerVlmUtils
{
    /**
     * Collects the backing storage device paths (LVM LVs, ZFS zvols, ...) for the given volume by
     * traversing the layer tree down to the {@link DeviceLayerKind#STORAGE} level. This includes every
     * storage-suffix branch (data, {@code .meta}, {@code .cache}, ...), so the returned set covers all
     * block devices that can show I/O on the volume's behalf during a mkfs / create-md. Diskless
     * branches carry no device path and are naturally skipped.
     *
     * <p>The returned paths still need to be resolved to their sysfs 'stat' files (satellite
     * {@code DeviceStatUtils}) before being handed to
     * {@link com.linbit.extproc.ChildProcessHandler#setIoProgressMode(boolean, java.util.Collection)}.</p>
     */
    public static <RSC extends AbsResource<RSC>> Set<String> getStorageDevicePaths(
        AbsRscLayerObject<RSC> rootRef,
        VolumeNumber vlmNrRef
    )
    {
        Set<String> devicePaths = new TreeSet<>();
        for (AbsRscLayerObject<RSC> storRscData : LayerRscUtils.getRscDataByLayer(rootRef, DeviceLayerKind.STORAGE))
        {
            @Nullable VlmProviderObject<RSC> vlmData = storRscData.getVlmLayerObjects().get(vlmNrRef);
            if (vlmData != null)
            {
                @Nullable String devicePath = vlmData.getDevicePath();
                if (devicePath != null && !devicePath.isEmpty())
                {
                    devicePaths.add(devicePath);
                }
            }
        }
        return devicePaths;
    }

    public static <RSC extends AbsResource<RSC>> Set<StorPool> getStorPools(AbsResource<RSC> absRscRef)
    {
        return getStorPools(absRscRef, true);
    }

    public static <RSC extends AbsResource<RSC>> Set<StorPool> getStorPools(
        AbsResource<RSC> absRscRef,
        boolean withMetaStoragePools
    )
    {
        Iterator<? extends AbsVolume<RSC>> vlmIt = absRscRef.iterateVolumes();
        Set<StorPool> storPools = new TreeSet<>();
        while (vlmIt.hasNext())
        {
            AbsVolume<RSC> vlm = vlmIt.next();
            storPools.addAll(getStorPoolSet(vlm, withMetaStoragePools));
        }
        return storPools;
    }

    public static <RSC extends AbsResource<RSC>> Set<StorPool> getStorPoolSet(
        AbsVolume<RSC> vlm,
        boolean withMetaData
    )
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        Set<AbsRscLayerObject<RSC>> storageRscDataSet = LayerRscUtils.getRscDataByLayer(
            vlm.getAbsResource().getLayerData(),
            DeviceLayerKind.STORAGE,
            withMetaData ?
                layerSuffix -> true :
                RscLayerSuffixes::isNonMetaDataLayerSuffix
        );
        return getStoragePools(vlmNr, storageRscDataSet);
    }

    private static <RSC extends AbsResource<RSC>> Set<StorPool> getStoragePools(
        VolumeNumber vlmNr, Set<AbsRscLayerObject<RSC>> storageRscDataSet)
    {
        Set<StorPool> storPools = new TreeSet<>();
        for (AbsRscLayerObject<RSC> rscData : storageRscDataSet)
        {
            VlmProviderObject<RSC> vlmProviderObject = rscData.getVlmProviderObject(vlmNr);
            if (vlmProviderObject != null)
            {
                /*
                 *  vlmProviderObject is null in the following usecase:
                 *
                 *  DRBD with 2 volumes,
                 *      one has external meta-data, the other has internal
                 *
                 *  this will create 2 STORAGE resources ("", and ".meta")
                 *      "" will have 2 vlmProviderObjects (as usual)
                 *      ".meta" will only have 1 vlmProviderObject, as the other has internal metadata
                 */
                storPools.add(vlmProviderObject.getStorPool());
            }
        }
        return storPools;
    }

    public static Set<StorPool> getStorPoolSet(VlmProviderObject<?> vlmData)
    {
        return getStoragePools(
            vlmData.getVlmNr(), LayerRscUtils.getRscDataByLayer(
            vlmData.getRscLayerObject(),
                DeviceLayerKind.STORAGE
            )
        );
    }

    public static <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> Map<String, StorPool> getStorPoolMap(
        VLM vlm
    )
    {
        return getStorPoolMap(
            vlm.getAbsResource(),
            vlm.getVolumeNumber()
        );
    }

    public static <RSC extends AbsResource<RSC>> Map<String, StorPool> getStorPoolMap(
        RSC rsc,
        VolumeNumber vlmNr
    )
    {
        Map<String, StorPool> storPoolMap = new TreeMap<>();
        List<AbsRscLayerObject<RSC>> storageRscList = LayerUtils.getChildLayerDataByKind(
            rsc.getLayerData(),
            DeviceLayerKind.STORAGE
        );
        for (AbsRscLayerObject<RSC> storageRsc : storageRscList)
        {
            VlmProviderObject<RSC> storageVlmData = storageRsc.getVlmProviderObject(vlmNr);
            if (storageVlmData != null)
            {
                /*
                 *  storageVlmData is null in the following usecase:
                 *
                 *  DRBD with 2 volumes,
                 *      one has external meta-data, the other has internal
                 *
                 *  this will create 2 STORAGE resources ("", and ".meta")
                 *      "" will have 2 storageVlmData (as usual)
                 *      ".meta" will only have 1 storageVlmData, as the other has internal metadata
                 */
                storPoolMap.put(
                    storageRsc.getResourceNameSuffix(),
                    storageVlmData.getStorPool()
                );
            }
        }
        return storPoolMap;

    }

    private LayerVlmUtils()
    {
    }
}
