package com.linbit.linstor.storage.layer.kinds;

import java.util.HashMap;
import java.util.Map;

public class DeviceLayerKindStackUtils
{
    private static final LayerNode START_NODE = new LayerNode();
    private static final Map<Class<? extends DeviceLayerKind>, LayerNode> NODES_LUT = new HashMap<>();

    static
    {
        START_NODE
            // .addSuccessor(BareMetalLayerKind.class)
            // .addSuccessor(ZfsLayerKind.class)
            // .addSuccessor(ZfsThinLayerKind.class)
            // .addSuccessor(LvmThinLayerKind.class)
            .addSuccessor(StorageLayerKind.class);

        // forLayerKind(BareMetalLayerKind.class)
        // .addSuccessor(DrbdLayerKind.class)
        // .addSuccessor(CryptSetupLayerKind.class);
        // forLayerKind(ZfsLayerKind.class)
        // .addSuccessor(DrbdLayerKind.class)
        // .addSuccessor(CryptSetupLayerKind.class);
        // forLayerKind(ZfsThinLayerKind.class)
        // .addSuccessor(DrbdLayerKind.class)
        // .addSuccessor(CryptSetupLayerKind.class);
        // forLayerKind(LvmThinLayerKind.class)
        // .addSuccessor(DrbdLayerKind.class)
        // .addSuccessor(CryptSetupLayerKind.class);
        forLayerKind(StorageLayerKind.class)
            .addSuccessor(DrbdLayerKind.class)
            .addSuccessor(CryptSetupLayerKind.class);

        forLayerKind(CryptSetupLayerKind.class)
            .addSuccessor(DrbdLayerKind.class);
    }

    public static boolean isLayerKindStackAllowed(DeviceLayerKind[] kinds)
    {
        LayerNode currentLayer = START_NODE;
        if (kinds.length == 0)
        {
            currentLayer = null;
        }
        for (int idx = 0; idx < kinds.length && currentLayer != null; ++idx)
        {
            currentLayer = currentLayer.successor.get(kinds[idx].getClass());
        }

        return currentLayer != null;
    }

    /**
     * Although this method is very simple, it has the (more or less) convenient side-effect that
     * it returns a null value for not (yet) known {@link DeviceLayerKind}s. That means, that you cannot
     * have non-reachable nodes in this dependency graph. Every layer has to be reachable from some
     * predecessor node (which can be the virtual START_NODE).
     * @param kind
     * @return
     */
    private static LayerNode forLayerKind(Class<? extends DeviceLayerKind> kind)
    {
        return NODES_LUT.get(kind);
    }

    private static class LayerNode
    {
        private Map<Class<? extends DeviceLayerKind>, LayerNode> successor = new HashMap<>();

        private LayerNode addSuccessor(Class<? extends DeviceLayerKind> kind)
        {
            successor.put(kind, NODES_LUT.computeIfAbsent(kind, (ignore) -> new LayerNode()));
            return this;
        }
    }

}
