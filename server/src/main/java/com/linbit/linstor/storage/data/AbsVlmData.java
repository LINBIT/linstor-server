package com.linbit.linstor.storage.data;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Objects;

public abstract class AbsVlmData<RSC extends AbsResource<RSC>, RSC_DATA extends AbsRscLayerObject<RSC>> extends
    BaseTransactionObject
    implements Comparable<AbsVlmData<RSC, RSC_DATA>>
{
    protected final AbsVolume<RSC> vlm;
    protected final RSC_DATA rscData;

    public AbsVlmData(
        AbsVolume<RSC> vlmRef,
        RSC_DATA rscDataRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);
    }

    public AbsVolume<RSC> getVolume()
    {
        return vlm;
    }

    public RSC_DATA getRscLayerObject()
    {
        return rscData;
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
    public int compareTo(AbsVlmData<RSC, RSC_DATA> other)
    {
        int compareTo = 0;
        AbsVolume<RSC> otherVolume = other.vlm;
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
            throw new ImplementationError(
                "Unknown (other volume) AbsVolume class: " + otherVolume.getClass() +
                    " (local volume: " + vlm.getClass() + ")"
            );
        }
        if (compareTo == 0)
        {
            compareTo = rscData.getRscLayerId() - other.rscData.getRscLayerId();
        }
        return compareTo;
    }
}
