package com.linbit.linstor.storage.data.provider.utils;

import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.LinkedList;

public class ProviderUtils
{
    public static long getAllocatedSize(Volume vlm, AccessContext accCtx) throws AccessDeniedException
    {
        long sum = 0;
        Resource rsc = vlm.getResource();

        VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();

        LinkedList<RscLayerObject> rscDataList = new LinkedList<>();
        rscDataList.add(rsc.getLayerData(accCtx));

        while (!rscDataList.isEmpty())
        {
            RscLayerObject rscData = rscDataList.poll();
            rscDataList.addAll(rscData.getChildren());

            VlmProviderObject vlmData = rscData.getVlmProviderObject(vlmNr);
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
