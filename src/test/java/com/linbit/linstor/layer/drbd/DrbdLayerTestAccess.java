package com.linbit.linstor.layer.drbd;

import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Collection;

/** Gives tests outside this package access to package-private helpers of {@link DrbdLayer}. */
public final class DrbdLayerTestAccess
{
    private DrbdLayerTestAccess()
    {
    }

    public static boolean isCreatedByStlt(Collection<VlmProviderObject<?>> storageDevicesRef)
    {
        return DrbdLayer.isCreatedByStlt(storageDevicesRef);
    }
}
