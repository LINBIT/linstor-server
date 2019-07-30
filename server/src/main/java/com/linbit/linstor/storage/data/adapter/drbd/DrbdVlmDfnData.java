package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
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

import javax.inject.Provider;
import java.util.Arrays;
import java.util.Objects;

public class DrbdVlmDfnData extends BaseTransactionObject implements DrbdVlmDfnObject
{
    // unmodifiable data, once initialized
    private final VolumeDefinition vlmDfn;
    private final MinorNumber minorNr;
    private final String suffixedResourceName;
    private final String resourceNameSuffix;
    private final DrbdLayerDatabaseDriver dbDriver;
    private final DynamicNumberPool minorPool;
    private final DrbdRscDfnData drbdRscDfn;

    public DrbdVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String resourceNameSuffixRef,
        Integer minorRef,
        DynamicNumberPool minorPoolRef,
        DrbdRscDfnData drbdRscDfnRef,
        DrbdLayerDatabaseDriver dbDriverRef,
        Provider<TransactionMgr> transMgrProvider
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException
    {
        super(transMgrProvider);
        resourceNameSuffix = resourceNameSuffixRef;
        minorPool = minorPoolRef;
        drbdRscDfn = drbdRscDfnRef;
        dbDriver = dbDriverRef;
        suffixedResourceName = vlmDfnRef.getResourceDefinition().getName().displayValue + resourceNameSuffixRef;

        if (minorRef == null)
        {
            minorNr = new MinorNumber(minorPool.autoAllocate());
        }
        else
        {
            minorNr = new MinorNumber(minorRef);
            minorPoolRef.allocate(minorRef);
        }

        vlmDfn = Objects.requireNonNull(vlmDfnRef);

        transObjs = Arrays.asList(
            vlmDfn
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public VolumeDefinition getVolumeDefinition()
    {
        return vlmDfn;
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
        minorPool.deallocate(minorNr.value);
        dbDriver.delete(this);
        drbdRscDfn.delete(this);
    }

    @Override
    public DrbdVlmDfnPojo getApiData(AccessContext accCtxRef)
    {
        return new DrbdVlmDfnPojo(
            resourceNameSuffix,
            vlmDfn.getVolumeNumber().value,
            minorNr.value
        );
    }
}
