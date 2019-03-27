package com.linbit.linstor.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.DRBD;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.LUKS;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class LayerUtils
{
    private static final LayerNode TOPMOST_NODE = new LayerNode();

    private static final Map<DeviceLayerKind, LayerNode> NODES = new HashMap<>();

    static
    {
        TOPMOST_NODE.addChildren(DRBD, LUKS, STORAGE);

        NODES.get(DRBD).addChildren(LUKS, STORAGE);
        NODES.get(LUKS).addChildren(STORAGE);

        NODES.get(STORAGE).setAllowedEnd(true);


        ensureAllRulesEndWithStorage();
    }

    public static boolean isLayerKindStackAllowed(List<DeviceLayerKind> kindList)
    {
        LayerNode currentLayer = TOPMOST_NODE;
        boolean allowed = false;
        if (kindList.isEmpty())
        {
            currentLayer = null;
        }
        else
        {
            allowed = true;
            for (DeviceLayerKind kind : kindList)
            {
                currentLayer = currentLayer.successor.get(kind);
                if (currentLayer == null)
                {
                    allowed = false;
                    break;
                }
            }
            if (allowed && currentLayer != null && !currentLayer.endAllowed)
            {
                allowed = false;
            }
        }
        return allowed;
    }

    private static void ensureAllRulesEndWithStorage()
    {
        if (!NODES.get(STORAGE).successor.isEmpty())
        {
            throw new ImplementationError("STORAGE is not allowed to have children!");
        }
        for (DeviceLayerKind kind : TOPMOST_NODE.successor.keySet())
        {
            ensureAllRulesEndWithStorage(new LinkedList<>(), kind);
        }
    }

    private static void ensureAllRulesEndWithStorage(LinkedList<DeviceLayerKind> kindList, DeviceLayerKind kindRef)
    {
        kindList.add(kindRef);
        LayerNode layerNode = NODES.get(kindRef);
        if (layerNode == null)
        {
            throw new ImplementationError("Unknown kind: " + kindRef + " " + kindList);
        }
        if (!kindRef.equals(STORAGE))
        {
            Set<DeviceLayerKind> children = layerNode.successor.keySet();
            if (children.isEmpty())
            {
                throw new ImplementationError(kindList + " not ending with STORAGE layer");
            }
            for (DeviceLayerKind child : children)
            {
                ensureAllRulesEndWithStorage(kindList, child);
            }
        }
        kindList.removeLast();
    }

    private static class LayerNode
    {
        private Map<DeviceLayerKind, LayerNode> successor = new HashMap<>();
        private boolean endAllowed = false;

        private LayerNode addChildren(DeviceLayerKind... kinds)
        {
            for (DeviceLayerKind kind : kinds)
            {
                successor.put(kind, NODES.computeIfAbsent(kind, (ignore) -> new LayerNode()));
            }
            return this;
        }

        public void setAllowedEnd(boolean endAllowedRef)
        {
            endAllowed = endAllowedRef;
        }
    }

    public static List<RscLayerObject> getChildLayerDataByKind(
        RscLayerObject rscDataRef,
        DeviceLayerKind kind
    )
    {
        LinkedList<RscLayerObject> rscDataToExplore = new LinkedList<>();
        if (rscDataRef != null)
        {
            rscDataToExplore.add(rscDataRef);
        }

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

    public static boolean hasLayer(
        RscLayerObject rscData,
        DeviceLayerKind kind
    )
    {
        return !LayerUtils.getChildLayerDataByKind(rscData, kind).isEmpty();
    }

    public static Set<DeviceLayerKind> getUsedDeviceLayerKinds(RscLayerObject rscLayerObject)
    {
        Set<DeviceLayerKind> ret = new TreeSet<>();
        LinkedList<RscLayerObject> rscDataToExplore = new LinkedList<>();
        rscDataToExplore.add(rscLayerObject);
        while (!rscDataToExplore.isEmpty())
        {
            RscLayerObject rscData = rscDataToExplore.removeFirst();
            ret.add(rscData.getLayerKind());
            rscDataToExplore.addAll(rscData.getChildren());
        }
        return ret;
    }

}
