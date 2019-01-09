package com.linbit.linstor.storage.layer.provider.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ProviderUtils
{
    public static long getAllocatedSize(VlmProviderObject vlmData, ExtCmd extCmd)
        throws StorageException
    {
        return Commands.getBlockSizeInKib(extCmd, vlmData.getDevicePath());
    }

    public static long getAllocatedSize(Volume vlm, AccessContext accCtx) throws AccessDeniedException
    {
        long sum = 0;
        List<DeviceLayerKind> layerStack = vlm.getLayerStack(accCtx);
        Resource rsc = vlm.getResource();

        Queue<RscLayerObject> rscDataQueue = null;

        VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();

        for (DeviceLayerKind kind : layerStack)
        {
            if (rscDataQueue == null)
            {
                rscDataQueue = new LinkedList<>();
                rscDataQueue.add(rsc.getLayerData(accCtx, kind));
            }
            while (!rscDataQueue.isEmpty())
            {
                RscLayerObject rscData = rscDataQueue.poll();
                rscDataQueue.addAll(rscData.getChildren());

                VlmProviderObject vlmData = rscData.getVlmProviderObject(vlmNr);
                if (!(vlmData instanceof VlmLayerObject))
                {
                    sum += vlmData.getAllocatedSize();
                }
            }
        }

        return sum;
    }
}
