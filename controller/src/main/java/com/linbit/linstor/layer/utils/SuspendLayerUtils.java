package com.linbit.linstor.layer.utils;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>Decides which layers of a resource receive the suspend IO flag before a snapshot or clone is taken.</p>
 * <p>NOTE: the topmost layer will always receive the suspend IO boolean, regardless what kind of layer it is</p>
 * <p>Cache layers below the topmost layer (WRITECACHE, CACHE) are intentionally not suspended here: they flush their
 * cache in the satellite directly before the storage snapshot is taken (round trip 2). Suspending a dm-cache device
 * below DRBD already in round trip 1 blocks DRBD's own writes to it, which can stall the suspend-io of the peers.</p>
 * <p>This is the same deadlock as GH #540 (LUKS below DRBD): a node that suspends its DRBD and the layers below
 * faster than a Primary peer can no longer write the replicated data that is still in flight from that peer. Unlike
 * LUKS, cache layers have a state (the content of the cache device), so simply never flushing them is no option:
 * data that is still in the cache when the storage snapshot is taken is missing in the snapshot of the data device
 * (e.g. for backup shipping, which only ships the data device). Flushing in round trip 2 is safe, since that round
 * trip only starts after all nodes confirmed that their DRBD is suspended, so no more writes from peers can arrive
 * at the cache after the flush.</p>
 */
public class SuspendLayerUtils
{
    /**
     * A set of layers that needs to be suspended even when they are not the topmost layer. Only DRBD is left here,
     * which is currently not supported as a sub-root layer anyway. Cache layers must not be added again, see the
     * class comment.
     */
    private static final Set<DeviceLayerKind> LAYERS_TO_SUSPEND_SUB_ROOT;
    private static final Set<DeviceLayerKind> ALL_LAYERS;

    static
    {
        Set<DeviceLayerKind> set = new HashSet<>();
        set.add(DeviceLayerKind.DRBD); // just to be sure...
        LAYERS_TO_SUSPEND_SUB_ROOT = Collections.unmodifiableSet(set);

        set = new HashSet<>();
        set.addAll(Arrays.asList(DeviceLayerKind.values()));
        ALL_LAYERS = Collections.unmodifiableSet(set);
    }

    public static <RSC extends AbsResource<RSC>> void suspendIo(RSC rscRef)
        throws DatabaseException
    {
        setShouldSuspendStateRec(rscRef.getLayerData(), true, LAYERS_TO_SUSPEND_SUB_ROOT);
    }

    public static <RSC extends AbsResource<RSC>> void resumeIo(RSC rscRef)
        throws DatabaseException
    {
        setShouldSuspendStateRec(rscRef.getLayerData(), false, ALL_LAYERS);
    }

    public static <RSC extends AbsResource<RSC>> void setSuspend(
        RSC rscRef,
        boolean suspendRef
    )
        throws DatabaseException
    {
        if (suspendRef)
        {
            SuspendLayerUtils.suspendIo(rscRef);
        }
        else
        {
            SuspendLayerUtils.resumeIo(rscRef);
        }
    }

    private static <RSC extends AbsResource<RSC>> void setShouldSuspendStateRec(
        AbsRscLayerObject<RSC> rootRscLayerObjRef,
        boolean suspendStateRef,
        Set<DeviceLayerKind> layersToApply
    )
        throws DatabaseException
    {
        // the topmost layer always receives the flag, lower layers only if they are in layersToApply
        rootRscLayerObjRef.setShouldSuspendIo(suspendStateRef);
        setShouldSuspendStateOfChildrenRec(rootRscLayerObjRef, suspendStateRef, layersToApply);
    }

    private static <RSC extends AbsResource<RSC>> void setShouldSuspendStateOfChildrenRec(
        AbsRscLayerObject<RSC> absRscLayerObjRef,
        boolean suspendStateRef,
        Set<DeviceLayerKind> layersToApply
    )
        throws DatabaseException
    {
        for (AbsRscLayerObject<RSC> child : absRscLayerObjRef.getChildren())
        {
            if (layersToApply.contains(child.getLayerKind()))
            {
                child.setShouldSuspendIo(suspendStateRef);
            }
            setShouldSuspendStateOfChildrenRec(child, suspendStateRef, layersToApply);
        }
    }

    private SuspendLayerUtils()
    {
    }
}
