package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerDrbdVlmDfnDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmDfnObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;

public class DrbdVlmDfnData<RSC extends AbsResource<RSC>>
    extends BaseTransactionObject
    implements DrbdVlmDfnObject
{
    public static final int SNAPSHOT_MINOR = -1;

    // unmodifiable data, once initialized
    private final @Nullable VolumeDefinition vlmDfn;
    private final VolumeNumber vlmNr;
    private final ResourceName rscName;
    private final @Nullable SnapshotName snapName;
    private final @Nullable MinorNumber minorNr;
    private final String suffixedResourceName;
    private final String resourceNameSuffix;
    private final LayerDrbdVlmDfnDatabaseDriver dbDriver;
    private final DynamicNumberPool minorPool;
    private final DrbdRscDfnData<RSC> drbdRscDfn;

    public DrbdVlmDfnData(
        @Nullable VolumeDefinition vlmDfnRef,
        ResourceName rscNameRef,
        @Nullable SnapshotName snapNameRef,
        String resourceNameSuffixRef,
        VolumeNumber vlmNrRef,
        @Nullable Integer minorRef,
        DynamicNumberPool minorPoolRef,
        DrbdRscDfnData<RSC> drbdRscDfnRef,
        LayerDrbdVlmDfnDatabaseDriver dbDriverRef,
        Provider<? extends TransactionMgr> transMgrProvider
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        super(transMgrProvider);
        vlmDfn = vlmDfnRef;
        vlmNr = vlmNrRef;
        rscName = rscNameRef;
        snapName = snapNameRef;
        resourceNameSuffix = resourceNameSuffixRef;
        minorPool = minorPoolRef;
        drbdRscDfn = drbdRscDfnRef;
        dbDriver = dbDriverRef;
        suffixedResourceName = rscName.displayValue +
            (snapName == null ? "" : snapName.displayValue) + resourceNameSuffixRef;

        if (minorRef == null)
        {
            minorNr = new MinorNumber(minorPool.autoAllocate());
        }
        else
        {
            if (minorRef != SNAPSHOT_MINOR)
            {
                minorNr = new MinorNumber(minorRef);
                minorPoolRef.allocate(minorRef);
            }
            else
            {
                if (snapNameRef == null)
                {
                    throw new ImplementationError("Invalid minor number given for resource");
                }
                minorNr = null;
            }
        }

        transObjs = Arrays.asList(
            drbdRscDfn
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public ResourceName getResourceName()
    {
        return rscName;
    }

    @Override
    public @Nullable SnapshotName getSnapshotName()
    {
        return snapName;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        return vlmNr;
    }

    @Override
    public @Nullable MinorNumber getMinorNr()
    {
        return minorNr;
    }

    public String getSuffixedResourceName()
    {
        return suffixedResourceName;
    }

    @Override
    public String getRscNameSuffix()
    {
        return resourceNameSuffix;
    }

    @Override
    public void delete() throws DatabaseException
    {
        if (minorNr != null)
        {
            minorPool.deallocate(minorNr.value);
        }
        dbDriver.delete(this);
        drbdRscDfn.delete(this);
    }

    @Override
    public DrbdVlmDfnPojo getApiData(AccessContext accCtxRef)
    {
        return new DrbdVlmDfnPojo(
            resourceNameSuffix,
            vlmNr.value,
            minorNr == null ? null : minorNr.value
        );
    }

    public @Nullable VolumeDefinition getVolumeDefinition()
    {
        return vlmDfn;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resourceNameSuffix, rscName, snapName, vlmNr);
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof DrbdVlmDfnData)
        {
            DrbdVlmDfnData<?> other = (DrbdVlmDfnData<?>) obj;
            ret = Objects.equals(resourceNameSuffix, other.resourceNameSuffix) &&
                Objects.equals(rscName, other.rscName) && Objects.equals(snapName, other.snapName) &&
                Objects.equals(vlmNr, other.vlmNr);
        }
        return ret;
    }
}
