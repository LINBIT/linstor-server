package com.linbit.linstor.utils.layer;

import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.Iterator;
import java.util.List;

public class LayerKindUtils
{
    /**
     * See hasSpecialLayers(List&lt;DeviceLayerKind&gt;).
     */
    public static boolean hasSpecialLayers(final ResourceDefinition rscDfn, final AccessContext accCtx)
        throws AccessDeniedException
    {
        final List<DeviceLayerKind> layerStack = rscDfn.getLayerStack(accCtx);
        return hasSpecialLayers(layerStack);
    }

    /**
     * Indicates whether a layer stack includes any special (non-DRBD, non-storage) layers.
     *
     * @param layerStack The layer stack to inspect
     * @return True if the layer stack contains layers other than DeviceLayerKind.DRBD or DeviceLayerKind.STORAGE
     */
    public static boolean hasSpecialLayers(final List<DeviceLayerKind> layerStack)
    {
        final Iterator<DeviceLayerKind> layerIter = layerStack.iterator();
        return hasSpecialLayers(layerIter);
    }

    /**
     * See hasSpecialLayers(List&lt;DeviceLayerKind&gt;).
     */
    public static boolean hasSpecialLayers(final Iterator<DeviceLayerKind> layerIter)
    {
        // Determine whether the resource's layer stack has any non-storage/non-DRBD layers
        // Any such layers cause the resource's volumes to have a different min-io-size
        boolean isSpecial = false;
        while (!isSpecial && layerIter.hasNext())
        {
            final DeviceLayerKind layerKind = layerIter.next();
            isSpecial = layerKind != DeviceLayerKind.DRBD && layerKind != DeviceLayerKind.STORAGE;
        }
        return isSpecial;
    }
}
