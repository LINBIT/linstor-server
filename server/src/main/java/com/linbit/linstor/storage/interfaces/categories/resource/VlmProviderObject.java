package com.linbit.linstor.storage.interfaces.categories.resource;

import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.LayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.ExceptionThrowingSupplier;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;

/**
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 * Main difference between this interface and {@link VlmLayerObject} is that
 * this interface does not have {@link VlmLayerObject#getDataDevice()} and
 * {@link LayerObject#getChildren()} methods
 */
public interface VlmProviderObject<RSC extends AbsResource<RSC>> extends LayerObject, Comparable<VlmProviderObject<RSC>>
{
    long UNINITIALIZED_SIZE = -1;
    enum Size
    {
        TOO_SMALL,
        TOO_LARGE,
        TOO_LARGE_WITHIN_TOLERANCE,
        AS_EXPECTED
    }

    DeviceProviderKind getProviderKind();

    boolean exists();

    void setExists(boolean existsRef) throws DatabaseException;

    boolean hasFailed();

    /**
     * Only the VlmProviderObject of the topmost layer needs to store this data.
     * This size should usually be the same as the vlmDfn's size, expect
     * when a resize happens.
     */
    long getOriginalSize();

    void setOriginalSize(long originalSizeRef);

    long getAllocatedSize();

    void setAllocatedSize(long allocatedSizeRef) throws DatabaseException;

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

    long getDiscGran();

    void setDiscGran(long discGranRef) throws DatabaseException;

    long getExpectedSize();

    void setExpectedSize(long expectedSizeRef);

    default void setActive(boolean activeRef)
    {
        // ignored (unless overridden) as this layer only considers Resource.Flags.INACTIVE, not per-vlmData
    }

    default boolean isActive(AccessContext accCtx) throws AccessDeniedException
    {
        boolean isActive = true; // snapshots are active by default
        if (getVolume().getAbsResource() instanceof Resource)
        {
            isActive = !((Resource) getVolume().getAbsResource()).getStateFlags()
                .isSet(accCtx, Resource.Flags.INACTIVE);
        }
        return isActive;
    }

    default String getVolumeKey()
    {
        AbsVolume<RSC> volume = getVolume();
        NodeName nodeName = volume.getAbsResource().getNode().getName();
        ResourceName rscName = volume.getResourceDefinition().getName();
        String rscNameSuffix = getRscLayerObject().getResourceNameSuffix();
        VolumeNumber volNr = getVlmNr();
        String key;
        if (getVolume().getAbsResource() instanceof Snapshot)
        {
            String snapName = ((Snapshot) getVolume().getAbsResource()).getSnapshotName().value;
            key = "snapVlm: " + nodeName.value + "/" + rscName.value + rscNameSuffix + "/" + volNr.value + "_" +
                snapName;
        }
        else
        {
            key = "vlm: " + nodeName.value + "/" + rscName.value + rscNameSuffix + "/" + volNr.value;
        }
        return key;
    }

    @Nullable String getCloneDevicePath();

    void setCloneDevicePath(@Nullable String cloneDevicePath);

    @Override
    default int compareTo(VlmProviderObject<RSC> oRef)
    {
        return getVolumeKey().compareTo(oRef.getVolumeKey());
    }
}
