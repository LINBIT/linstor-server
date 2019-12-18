package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.DrbdLayerDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmDfnObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.Arrays;

public class DrbdVlmDfnData<RSC extends AbsResource<RSC>>
    extends BaseTransactionObject
    implements DrbdVlmDfnObject
{
    public static final Integer SNAPSHOT_MINOR = -1;

    // unmodifiable data, once initialized
    private final VolumeDefinition vlmDfn;
    private final VolumeNumber vlmNr;
    private final ResourceName rscName;
    private final SnapshotName snapName;
    private final MinorNumber minorNr;
    private final String suffixedResourceName;
    private final String resourceNameSuffix;
    private final DrbdLayerDatabaseDriver dbDriver;
    private final DynamicNumberPool minorPool;
    private final DrbdRscDfnData<RSC> drbdRscDfn;


    public DrbdVlmDfnData(
        @Nullable VolumeDefinition vlmDfnRef,
        ResourceName rscNameRef,
        @Nullable SnapshotName snapNameRef,
        String resourceNameSuffixRef,
        VolumeNumber vlmNrRef,
        Integer minorRef,
        DynamicNumberPool minorPoolRef,
        DrbdRscDfnData<RSC> drbdRscDfnRef,
        DrbdLayerDatabaseDriver dbDriverRef,
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
    public SnapshotName getSnapshotName()
    {
        return snapName;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        return vlmNr;
    }

    @Override
    public MinorNumber getMinorNr()
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

    public VolumeDefinition getVolumeDefinition()
    {
        return vlmDfn;
    }
}
