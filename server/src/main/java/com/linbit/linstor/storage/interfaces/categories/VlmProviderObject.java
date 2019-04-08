package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.ExceptionThrowingSupplier;

import javax.annotation.Nullable;

import java.util.List;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 * Main difference between this interface and {@link VlmLayerObject} is that
 * this interface does not have {@link VlmLayerObject#getBackingDevice()} and
 * {@link LayerObject#getChildren()} methods
 */
public interface VlmProviderObject extends LayerObject
{
    enum Size
    {
        TOO_SMALL,
        TOO_LARGE,
        TOO_LARGE_WITHIN_TOLERANCE,
        AS_EXPECTED
    }

    DeviceProviderKind getProviderKind();

    boolean exists();

    boolean hasFailed();

    long getAllocatedSize();

    Volume getVolume();

    default VolumeNumber getVlmNr()
    {
       return getVolume().getVolumeDefinition().getVolumeNumber();
    }

    @Nullable VlmDfnLayerObject getVlmDfnLayerObject();

    RscLayerObject getRscLayerObject();

    default long getParentAllocatedSizeOrElse(ExceptionThrowingSupplier<Long, AccessDeniedException> orElse)
        throws AccessDeniedException
    {
        long ret;
        RscLayerObject parent = getRscLayerObject().getParent();
        if (parent != null)
        {
            ret = parent.getVlmProviderObject(getVlmNr()).getAllocatedSize();
        }
        else
        {
            ret = orElse.supply();
        }
        return ret;
    }

    long getUsableSize();

    // can be null if the layer cannot provide that device even in non-error states
    // e.g: VG / LV inactivate, DRBD secondary, missing crypt password, ...
    @Nullable String getDevicePath();

    Size getSizeState();

    List<? extends State> getStates();

    String getIdentifier();

    VlmLayerDataApi asPojo(AccessContext accCtxRef);
}
