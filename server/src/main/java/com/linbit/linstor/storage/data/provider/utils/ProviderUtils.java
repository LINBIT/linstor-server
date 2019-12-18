package com.linbit.linstor.storage.data.provider.utils;

import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.LinkedList;

public class ProviderUtils
{
    public static <RSC extends AbsResource<RSC>, VLM extends AbsVolume<RSC>> long getAllocatedSize(
        VLM vlm,
        AccessContext accCtx
    ) throws AccessDeniedException
    {
        long sum = 0;
        RSC rsc = vlm.getAbsResource();

        VolumeNumber vlmNr = vlm.getVolumeNumber();

        LinkedList<AbsRscLayerObject<RSC>> rscDataList = new LinkedList<>();
        rscDataList.add(rsc.getLayerData(accCtx));

        while (!rscDataList.isEmpty())
        {
            AbsRscLayerObject<RSC> rscData = rscDataList.poll();
            rscDataList.addAll(rscData.getChildren());

            VlmProviderObject<RSC> vlmData = rscData.getVlmProviderObject(vlmNr);
            if (vlmData != null && !(vlmData instanceof VlmLayerObject))
            {
                sum += vlmData.getAllocatedSize();
            }
        }
        return sum;
    }

    private ProviderUtils()
    {
    }
}
