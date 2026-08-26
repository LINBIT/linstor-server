package com.linbit.linstor.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linbit.linstor.storage.kinds.DeviceLayerKind.BCACHE;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.CACHE;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.DRBD;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.LUKS;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.NVME;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.WRITECACHE;

public class LayerUtils
{
    private static final LayerNode TOPMOST_NODE = new LayerNode(null);

    private static final Map<DeviceLayerKind, LayerNode> NODES = new HashMap<>();

    static
    {
        TOPMOST_NODE.addChildren(DRBD, LUKS, STORAGE, NVME, WRITECACHE, CACHE, BCACHE);

        NODES.get(DRBD).addChildren(NVME, LUKS, STORAGE, WRITECACHE, CACHE, BCACHE);
        NODES.get(LUKS).addChildren(STORAGE);
        NODES.get(NVME).addChildren(LUKS, STORAGE, WRITECACHE, CACHE, BCACHE);
        NODES.get(WRITECACHE).addChildren(NVME, LUKS, STORAGE, CACHE, BCACHE);
        NODES.get(CACHE).addChildren(NVME, LUKS, STORAGE, WRITECACHE, BCACHE);
        NODES.get(BCACHE).addChildren(NVME, LUKS, STORAGE, WRITECACHE);

        // "every layerlist has to end with STORAGE"
        NODES.get(STORAGE).setAllowedEnd(true);

        ensureAllRulesHaveAllowedEnd();
    }

    /**
     * TODO: This check needs to be extended by giving a (or multiple?) storage pools as some layer-combinations are
     * only allowed with specific DeviceProviderKinds.
     * For example 'nvme,luks,storage' is only allowed if Linstor is actually in full control of the storage, like LVM
     * or ZFS but NOT with (remote-)SPDK, etc...
     */
    public static boolean isLayerKindStackAllowed(List<DeviceLayerKind> kindList)
    {
        boolean allowed = false;

        // check for duplicates
        Set<DeviceLayerKind> duplicateCheck = new HashSet<>(kindList);
        if (duplicateCheck.size() == kindList.size())
        {
            LayerNode currentLayer = TOPMOST_NODE;
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
        }
        return allowed;
    }

    private static void ensureAllRulesHaveAllowedEnd()
    {
        Set<LayerNode> allowedEndNodes = new HashSet<>();
        Set<LayerNode> nodesToCheck = new HashSet<>();
        for (LayerNode node : NODES.values())
        {
            if (node.endAllowed)
            {
                allowedEndNodes.add(node);
            }
            else
            {
                nodesToCheck.add(node);
            }
        }

        int lastCheckCount = NODES.size();
        while (lastCheckCount != nodesToCheck.size())
        {
            Set<LayerNode> nextNodesToCheck = new HashSet<>();
            for (LayerNode node : nodesToCheck)
            {
                boolean allowed = false;
                for (LayerNode successor : node.successor.values())
                {
                    if (allowedEndNodes.contains(successor))
                    {
                        allowedEndNodes.add(node);
                        allowed = true;
                        break;
                    }
                }
                if (!allowed)
                {
                    nextNodesToCheck.add(node);
                }
            }
            nodesToCheck = nextNodesToCheck;
            lastCheckCount = nodesToCheck.size();
        }

        if (!nodesToCheck.isEmpty())
        {
            List<String> nodeDevs = new ArrayList<>();
            for (LayerNode node : nodesToCheck)
            {
                nodeDevs.add(node.kind.name());
            }
            throw new ImplementationError("Kind(s) " + nodeDevs + " do not have allowed ending");
        }
    }

    private static class LayerNode
    {
        private Map<DeviceLayerKind, LayerNode> successor = new HashMap<>();
        private boolean endAllowed = false;
        private @Nullable DeviceLayerKind kind;

        LayerNode(@Nullable DeviceLayerKind kindRef)
        {
            kind = kindRef;
        }

        private LayerNode addChildren(DeviceLayerKind... kinds)
        {
            for (DeviceLayerKind curKind : kinds)
            {
                successor.put(curKind, NODES.computeIfAbsent(curKind, (ignore) -> new LayerNode(curKind)));
            }
            return this;
        }

        private void setAllowedEnd(boolean endAllowedRef)
        {
            endAllowed = endAllowedRef;
        }
    }

    public static <RSC extends AbsResource<RSC>> List<AbsRscLayerObject<RSC>> getChildLayerDataByKind(
        AbsRscLayerObject<RSC> rscDataRef,
        DeviceLayerKind kind
    )
    {
        ArrayDeque<AbsRscLayerObject<RSC>> rscDataToExplore = new ArrayDeque<>();
        if (rscDataRef != null)
        {
            rscDataToExplore.add(rscDataRef);
        }

        List<AbsRscLayerObject<RSC>> ret = new ArrayList<>();

        while (!rscDataToExplore.isEmpty())
        {
            AbsRscLayerObject<RSC> rscData = rscDataToExplore.removeFirst();
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

    public static boolean hasLayer(AbsRscLayerObject<?> rscData, DeviceLayerKind kind)
    {
        return !LayerUtils.getChildLayerDataByKind(rscData, kind).isEmpty();
    }

    public static <RSC extends AbsResource<RSC>> List<DeviceLayerKind> getUsedDeviceLayerKinds(
        AbsRscLayerObject<RSC> rscLayerObject
    )
    {
        List<DeviceLayerKind> usedLayers = new ArrayList<>();

        AbsRscLayerObject<RSC> curLayerObject = rscLayerObject;

        while (curLayerObject != null)
        {
            DeviceLayerKind kind = curLayerObject.getLayerKind();
            usedLayers.add(kind);

            boolean skipAboveUs = false;
            boolean skipBelowUs = false;

            if (DeviceLayerKind.NVME.equals(kind))
            {
                if (((NvmeRscData<?>) curLayerObject).isInitiator())
                {
                    skipBelowUs = true;
                }
                else
                {
                    skipAboveUs = true;
                }
            }
            else if (DeviceLayerKind.STORAGE.equals(kind))
            {
                // initialize with !vlmMap.isEmpty() since if we do not have any volumes yet, we also have no EBS.
                boolean allVlmsEbsTarget = !curLayerObject.getVlmLayerObjects().isEmpty();
                for (VlmProviderObject<?> vlmProviderObject : curLayerObject.getVlmLayerObjects().values())
                {
                    if (!vlmProviderObject.getProviderKind().equals(DeviceProviderKind.EBS_TARGET))
                    {
                        allVlmsEbsTarget = false;
                        break;
                    }
                    // we can skip checking for EBS_INITIATOR since we are already in the STORAGE layer - the
                    // bottom-most layer, so there is nothing below us we could skip
                }
                skipAboveUs = allVlmsEbsTarget;
            }
            if (skipBelowUs)
            {
                // we do not care about layers below us
                break;
            }
            if (skipAboveUs)
            {
                // we do not care about layers above us
                usedLayers.clear();
                usedLayers.add(kind);
            }
            curLayerObject = curLayerObject.getChildBySuffix("");
        }
        return usedLayers;
    }

    public static List<DeviceLayerKind> getLayerStack(AbsResource<?> rscRef)
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        AbsRscLayerObject<?> layerData = rscRef.getLayerData();
        while (layerData != null)
        {
            ret.add(layerData.getLayerKind());
            layerData = layerData.getChildBySuffix("");
        }
        return ret;
    }

    private LayerUtils()
    {
    }
}
