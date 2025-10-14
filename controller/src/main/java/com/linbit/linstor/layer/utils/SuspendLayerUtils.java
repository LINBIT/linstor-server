package com.linbit.linstor.layer.utils;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>If we want to suspend IO we do not only need to suspend IO for the topmost layer but in some cases also layers
 * below need to perform some operations (i.e. flush some caches) during the suspend IO process in order to properly
 * write all data to the data device (instead of having them linger around in some cache or meta devices).</p>
 * <p>This class is encapsulates the logic which layers need to perform some action.</p>
 * <p>NOTE: the topmost layer will always receive the suspend IO boolean, regardless what kind of layer it is</p>
 */
public class SuspendLayerUtils
{
    /**
     * A set of layers that needs to be suspended even when they are not the topmost layer.
     * This is mostly needed to flush some layer internal caches
     */
    private static final Set<DeviceLayerKind> LAYERS_TO_SUSPEND_SUB_ROOT;
    private static final Set<DeviceLayerKind> ALL_LAYERS;

    static
    {
        Set<DeviceLayerKind> set = new HashSet<>();
        set.add(DeviceLayerKind.WRITECACHE);
        set.add(DeviceLayerKind.CACHE);
        set.add(DeviceLayerKind.DRBD); // just to be sure...
        LAYERS_TO_SUSPEND_SUB_ROOT = Collections.unmodifiableSet(set);

        set = new HashSet<>();
        set.addAll(Arrays.asList(DeviceLayerKind.values()));
        ALL_LAYERS = Collections.unmodifiableSet(set);
    }

    public static <RSC extends AbsResource<RSC>> void suspendIo(AccessContext accCtxRef, RSC rscRef)
        throws DatabaseException, AccessDeniedException
    {
        setShouldSuspendStateRec(rscRef.getLayerData(accCtxRef), true, LAYERS_TO_SUSPEND_SUB_ROOT);
    }

    public static <RSC extends AbsResource<RSC>> void resumeIo(AccessContext accCtxRef, RSC rscRef)
        throws DatabaseException, AccessDeniedException
    {
        setShouldSuspendStateRec(rscRef.getLayerData(accCtxRef), false, ALL_LAYERS);
    }

    public static <RSC extends AbsResource<RSC>> void setSuspend(
        AccessContext accCtxRef,
        RSC rscRef,
        boolean suspendRef
    )
        throws DatabaseException, AccessDeniedException
    {
        if (suspendRef)
        {
            SuspendLayerUtils.suspendIo(accCtxRef, rscRef);
        }
        else
        {
            SuspendLayerUtils.resumeIo(accCtxRef, rscRef);
        }
    }

    private static <RSC extends AbsResource<RSC>> void setShouldSuspendStateRec(
        AbsRscLayerObject<RSC> absRscLayerObjRef,
        boolean suspendStateRef,
        Set<DeviceLayerKind> layersToApply
    )
        throws DatabaseException
    {
        absRscLayerObjRef.setShouldSuspendIo(suspendStateRef);
        for (AbsRscLayerObject<RSC> child : absRscLayerObjRef.getChildren())
        {
            if (layersToApply.contains(child.getLayerKind()))
            {
                child.setShouldSuspendIo(suspendStateRef);
            }
            setShouldSuspendStateRec(child, suspendStateRef, layersToApply);
        }
    }

    private SuspendLayerUtils()
    {
    }
}
