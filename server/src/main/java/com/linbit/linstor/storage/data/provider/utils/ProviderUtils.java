package com.linbit.linstor.storage.data.provider.utils;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
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
}
