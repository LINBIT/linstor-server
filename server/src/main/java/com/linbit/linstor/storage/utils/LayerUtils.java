package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class LayerUtils
{
    public static List<RscLayerObject> getChildLayerDataByKind(
        RscLayerObject rscDataRef,
        DeviceLayerKind kind
    )
    {
        LinkedList<RscLayerObject> rscDataToExplore = new LinkedList<>();
        rscDataToExplore.add(rscDataRef);

        List<RscLayerObject> ret = new ArrayList<>();

        while (!rscDataToExplore.isEmpty())
        {
            RscLayerObject rscData = rscDataToExplore.removeFirst();
            if (rscData.getLayerKind().equals(kind))
            {
                ret.add(rscData);
            }
            else
            {
                rscDataToExplore.addAll(rscData.getChildren());
            }
        }

        return ret;
    }
}
