package com.linbit.linstor.utils.layer;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class LayerRscUtils
{
    public static <RSC extends AbsResource<RSC>> Set<AbsRscLayerObject<RSC>> getRscDataByLayer(
        AbsRscLayerObject<RSC> rscLayerDataRef,
        DeviceLayerKind kind
    )
    {
        return getRscDataByLayer(rscLayerDataRef, kind, ignored -> true);
    }

    public static <RSC extends AbsResource<RSC>> Set<AbsRscLayerObject<RSC>> getRscDataByLayer(
        AbsRscLayerObject<RSC> rscLayerDataRef,
        DeviceLayerKind kind,
        Predicate<String> includeLayerSufix
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
                    if (includeLayerSufix.test(rscLayerObj.getResourceNameSuffix()))
                    {
                        rscLayerObjects.add(rscLayerObj);
                    }
                }
                else
                {
                    Set<AbsRscLayerObject<RSC>> children = rscLayerObj.getChildren();
                    nextToExpand.addAll(children);
                }
            }

            rscDataToExpand.clear();
            rscDataToExpand.addAll(nextToExpand);
        }
        return rscLayerObjects;
    }

    public static List<DeviceLayerKind> getLayerStack(Resource rscRef, AccessContext accCtx)
        throws AccessDeniedException
    {
        return getLayerStack(rscRef.getLayerData(accCtx), accCtx);
    }

    public static List<DeviceLayerKind> getLayerStack(AbsRscLayerObject<?> rootLayerObject, AccessContext accCtx)
        throws AccessDeniedException
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        AbsRscLayerObject<?> layerData = rootLayerObject;
        while (layerData != null)
        {
            ret.add(layerData.getLayerKind());
            layerData = layerData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
        }
        return ret;
    }

    private LayerRscUtils()
    {
    }
}
