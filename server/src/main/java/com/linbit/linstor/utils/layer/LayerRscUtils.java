package com.linbit.linstor.utils.layer;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LayerRscUtils
{
    public static <RSC extends AbsResource<RSC>> Set<AbsRscLayerObject<RSC>> getRscDataByProvider(
        AbsRscLayerObject<RSC> rscLayerDataRef,
        DeviceLayerKind kind
    )
    {
        Set<AbsRscLayerObject<RSC>> rscLayerObjects = new HashSet<>();

        List<AbsRscLayerObject<RSC>> rscDataToExpand = new ArrayList<>();
        rscDataToExpand.add(rscLayerDataRef);
        while (!rscDataToExpand.isEmpty())
        {
            List<AbsRscLayerObject<RSC>> nextToExpand = new ArrayList<>();
            for (AbsRscLayerObject<RSC> rscLayerObj : rscDataToExpand)
            {
                if (rscLayerObj.getLayerKind().equals(kind))
                {
                    rscLayerObjects.add(rscLayerObj);
                }
                else
                {
                    Set<AbsRscLayerObject<RSC>> children = rscLayerObj.getChildren();
                    if (children != null)
                    {
                        nextToExpand.addAll(children);
                    }
                }
            }

            rscDataToExpand.clear();
            rscDataToExpand.addAll(nextToExpand);
        }
        return rscLayerObjects;
    }

    public static List<DeviceLayerKind> getLayerStack(Resource rscRef, AccessContext accCtx)
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        try
        {
            AbsRscLayerObject<Resource> layerData = rscRef.getLayerData(accCtx);
            while (layerData != null)
            {
                ret.add(layerData.getLayerKind());
                layerData = layerData.getChildBySuffix("");
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    private LayerRscUtils()
    {
    }
}
