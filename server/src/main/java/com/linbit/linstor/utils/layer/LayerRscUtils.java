package com.linbit.linstor.utils.layer;

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

    private LayerRscUtils()
    {
    }
}
