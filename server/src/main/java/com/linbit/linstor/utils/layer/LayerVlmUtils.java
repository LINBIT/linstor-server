package com.linbit.linstor.utils.layer;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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
    public static <RSC extends AbsResource<RSC>> Set<StorPool> getStorPools(RSC absRscRef, AccessContext accCtxRef)
        throws AccessDeniedException
    {
        return getStorPools(absRscRef, accCtxRef, true);
    }

    public static <RSC extends AbsResource<RSC>> Set<StorPool> getStorPools(
        RSC absRscRef,
        AccessContext accCtxRef,
        boolean withMetaStoragePools
    )
        throws AccessDeniedException
    {
        Iterator<? extends AbsVolume<RSC>> vlmIt = absRscRef.iterateVolumes();
        Set<StorPool> storPools = new TreeSet<>();
        while (vlmIt.hasNext())
        {
            AbsVolume<RSC> vlm = vlmIt.next();
            storPools.addAll(getStorPoolSet(vlm, accCtxRef, withMetaStoragePools));
        }
        return storPools;
    }

    public static <RSC extends AbsResource<RSC>> Set<StorPool> getStorPoolSet(
        AbsVolume<RSC> vlm,
        AccessContext accCtx,
        boolean withMetaData
    )
        throws AccessDeniedException
    {
        VolumeNumber vlmNr = vlm.getVolumeNumber();

        Set<AbsRscLayerObject<RSC>> storageRscDataSet = LayerRscUtils.getRscDataByLayer(
            vlm.getAbsResource().getLayerData(accCtx),
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

    public static Set<StorPool> getStorPoolSet(VlmProviderObject<?> vlmData, AccessContext accCtx)
    {
        return getStoragePools(
            vlmData.getVlmNr(), LayerRscUtils.getRscDataByLayer(
            vlmData.getRscLayerObject(),
                DeviceLayerKind.STORAGE
            )
        );
    }

    public static <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> Map<String, StorPool> getStorPoolMap(
        VLM vlm,
        AccessContext accCtx
    )
    {
        return getStorPoolMap(
            vlm.getAbsResource(),
            vlm.getVolumeNumber(),
            accCtx
        );
    }

    public static <RSC extends AbsResource<RSC>> Map<String, StorPool> getStorPoolMap(
        RSC rsc,
        VolumeNumber vlmNr,
        AccessContext accCtx
    )
    {
        Map<String, StorPool> storPoolMap = new TreeMap<>();
        try
        {
            List<AbsRscLayerObject<RSC>> storageRscList = LayerUtils.getChildLayerDataByKind(
                rsc.getLayerData(accCtx),
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
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get storage pool of " + rsc + ", Volume number: " + vlmNr,
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        return storPoolMap;

    }

    private LayerVlmUtils()
    {
    }
}
