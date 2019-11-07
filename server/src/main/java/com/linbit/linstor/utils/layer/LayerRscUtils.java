package com.linbit.linstor.utils.layer;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LayerRscUtils
{
    public static Set<RscLayerObject> getRscDataByProvider(RscLayerObject rscLayerDataRef, DeviceLayerKind kind)
    {
        Set<RscLayerObject> rscLayerObjects = new HashSet<>();

        List<RscLayerObject> rscDataToExpand = new ArrayList<>();
        rscDataToExpand.add(rscLayerDataRef);
        while (!rscDataToExpand.isEmpty())
        {
            List<RscLayerObject> nextToExpand = new ArrayList<>();
            for (RscLayerObject rscLayerObj : rscDataToExpand)
            {
                if (rscLayerObj.getLayerKind().equals(kind))
                {
                    rscLayerObjects.add(rscLayerObj);
                }
                else
                {
                    Set<RscLayerObject> children = rscLayerObj.getChildren();
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
            RscLayerObject layerData = rscRef.getLayerData(accCtx);
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
