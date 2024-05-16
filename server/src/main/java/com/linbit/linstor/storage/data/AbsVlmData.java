package com.linbit.linstor.storage.data;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.Objects;

public abstract class AbsVlmData<RSC extends AbsResource<RSC>, RSC_DATA extends AbsRscLayerObject<RSC>> extends
    BaseTransactionObject
    implements VlmProviderObject<RSC>
{
    // unmodifiable data, once initialized
    protected final AbsVolume<RSC> vlm;
    protected final RSC_DATA rscData;

    // not persisted, serialized, ctrl and stlt
    protected final TransactionSimpleObject<AbsVlmData<RSC, RSC_DATA>, String> devicePath;
    protected final TransactionSimpleObject<AbsVlmData<RSC, RSC_DATA>, Long> allocatedSize;
    protected final TransactionSimpleObject<AbsVlmData<RSC, RSC_DATA>, Long> usableSize;
    protected final TransactionSimpleObject<AbsVlmData<RSC, RSC_DATA>, Boolean> exists;
    protected final TransactionSimpleObject<AbsVlmData<RSC, RSC_DATA>, Boolean> failed;
    protected final TransactionSimpleObject<AbsVlmData<RSC, RSC_DATA>, Long> discGran;

    // not persisted, not serialized, stlt only
    protected transient long originalSize = UNINITIALIZED_SIZE;

    // holds the clone device path while cloning in process
    // not serializable, not persisted, stlt only
    protected @Nullable String cloneDevicePath;

    public AbsVlmData(
        AbsVolume<RSC> vlmRef,
        RSC_DATA rscDataRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        exists = transObjFactory.createTransactionSimpleObject(this, false, null);
        failed = transObjFactory.createTransactionSimpleObject(this, false, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, UNINITIALIZED_SIZE, null);
        usableSize = transObjFactory.createTransactionSimpleObject(this, UNINITIALIZED_SIZE, null);
        discGran = transObjFactory.createTransactionSimpleObject(this, UNINITIALIZED_SIZE, null);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);
    }

    @Override
    public AbsVolume<RSC> getVolume()
    {
        return vlm;
    }

    // can be null if the layer cannot provide that device even in non-error states
    // e.g: VG / LV inactivate, DRBD secondary, missing crypt password, ...
    @Override
    public @Nullable String getDevicePath()
    {
        return devicePath.get();
    }

    public void setDevicePath(String devicePathRef) throws DatabaseException
    {
        devicePath.set(devicePathRef);
    }

    @Override
    public RSC_DATA getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public boolean exists()
    {
        return exists.get();
    }

    @Override
    public void setExists(boolean existsRef) throws DatabaseException
    {
        exists.set(existsRef);
    }

    @Override
    public boolean hasFailed()
    {
        return failed.get();
    }

    public void setFailed(boolean failedRef) throws DatabaseException
    {
        failed.set(failedRef);
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    @Override
    public void setAllocatedSize(long allocatedSizeRef) throws DatabaseException
    {
        allocatedSize.set(allocatedSizeRef);
    }

    @Override
    public long getUsableSize()
    {
        return usableSize.get();
    }

    @Override
    public void setUsableSize(long usableSizeRef) throws DatabaseException
    {
        usableSize.set(usableSizeRef);
    }

    @Override
    public long getDiscGran()
    {
        return discGran.get();
    }

    @Override
    public void setDiscGran(long discGranRef) throws DatabaseException
    {
        discGran.set(discGranRef);
    }

    @Override
    public void setExpectedSize(long expectedSizeRef)
    {
        throw new ImplementationError("This method should only be called for STORAGE volumes");
    }

    @Override
    public long getExpectedSize()
    {
        throw new ImplementationError("This method should only be called for STORAGE volumes");
    }

    @Override
    public @Nullable String getCloneDevicePath()
    {
        return cloneDevicePath;
    }

    @Override
    public void setCloneDevicePath(@Nullable String cloneDevicePathRef)
    {
        cloneDevicePath = cloneDevicePathRef;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rscData.getRscLayerId(), vlm.getVolumeNumber());
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof AbsVlmData)
        {
            AbsVlmData<?, ?> other = (AbsVlmData<?, ?>) obj;
            ret = Objects.equals(rscData.getRscLayerId(), other.rscData.getRscLayerId()) &&
                Objects.equals(vlm.getVolumeNumber(), other.vlm.getVolumeNumber());
        }
        return ret;
    }

    @Override
    public int compareTo(VlmProviderObject<RSC> other)
    {
        int compareTo = 0;
        AbsVolume<RSC> otherVolume = other.getVolume();
        if (vlm instanceof Volume && otherVolume instanceof Volume)
        {
            compareTo = ((Volume) vlm).compareTo((Volume) otherVolume);
        }
        else if (vlm instanceof SnapshotVolume && otherVolume instanceof SnapshotVolume)
        {
            compareTo = ((SnapshotVolume) vlm).compareTo((SnapshotVolume) otherVolume);
        }
        else
        {
            // we know that one of us is a Volume and the other a SnapshotVolume, or null
            if (vlm == null || otherVolume == null)
            {
                if (vlm == null && otherVolume == null)
                {
                    throw new ImplementationError("Both volumes are null");
                }
                if (vlm == null)
                {
                    throw new ImplementationError("Local volume is null, other is: " + otherVolume.getClass());
                }
                throw new ImplementationError("Other volume is null, local is: " + vlm.getClass());
            }

            // sort volumes before snapshotVolumes
            if (vlm instanceof Volume)
            {
                compareTo = -1;
            }
            else
            {
                compareTo = 1;
            }
        }
        if (compareTo == 0)
        {
            compareTo = rscData.getRscLayerId() - other.getRscLayerObject().getRscLayerId();
        }
        return compareTo;
    }
}
