package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.LayerObject;
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
public interface VlmProviderObject<RSC extends AbsResource<RSC>> extends LayerObject
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

    AbsVolume<RSC> getVolume();

    default VolumeNumber getVlmNr()
    {
        return getVolume().getVolumeNumber();
    }

    @Nullable VlmDfnLayerObject getVlmDfnLayerObject();

    AbsRscLayerObject<RSC> getRscLayerObject();

    default long getParentAllocatedSizeOrElse(ExceptionThrowingSupplier<Long, AccessDeniedException> orElse)
        throws AccessDeniedException
    {
        long ret;
        AbsRscLayerObject<RSC> parent = getRscLayerObject().getParent();
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

    void setUsableSize(long netSizeRef) throws DatabaseException;

    long getUsableSize();

    // can be null if the layer cannot provide that device even in non-error states
    // e.g: VG / LV inactivate, DRBD secondary, missing crypt password, ...
    @Nullable String getDevicePath();

    Size getSizeState();

    List<? extends State> getStates();

    String getIdentifier();

    VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException;

    StorPool getStorPool();

    void setStorPool(AccessContext accCtxRef, StorPool storPoolRef) throws DatabaseException, AccessDeniedException;

    default String getVolumeKey()
    {
        AbsVolume<RSC> volume = getVolume();
        NodeName nodeName = volume.getAbsResource().getNode().getName();
        ResourceName rscName = volume.getResourceDefinition().getName();
        String rscNameSuffix = getRscLayerObject().getResourceNameSuffix();
        VolumeNumber volNr = getVlmNr();
        return "vlm: " + nodeName.value + "/" + rscName.value + rscNameSuffix + "/" + volNr.value;
    }
}
