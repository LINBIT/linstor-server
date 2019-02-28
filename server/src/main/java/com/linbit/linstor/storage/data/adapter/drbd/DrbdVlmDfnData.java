package com.linbit.linstor.storage.data.adapter.drbd;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.pojo.DrbdRscPojo.DrbdVlmDfnPojo;
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

    public DrbdVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String resourceNameSuffixRef,
        MinorNumber minorRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        resourceNameSuffix = resourceNameSuffixRef;
        suffixedResourceName = vlmDfnRef.getResourceDefinition().getName().displayValue + resourceNameSuffixRef;
        minorNr = Objects.requireNonNull(minorRef);

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

    public DrbdVlmDfnPojo asPojo(AccessContext accCtxRef)
    {
        return new DrbdVlmDfnPojo(
            suffixedResourceName,
            vlmDfn.getVolumeNumber().value,
            minorNr.value
        );
    }
}
