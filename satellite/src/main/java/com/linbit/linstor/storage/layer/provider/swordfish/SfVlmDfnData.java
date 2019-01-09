package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.SfVlmDfnProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SfVlmDfnData extends BaseTransactionObject implements SfVlmDfnProviderObject
{
    final VolumeDefinition vlmDfn;
    final List<? extends State> unmodStates;

    transient boolean exists;
    final transient List<? extends State> states = new ArrayList<>();
    transient long size;
    String vlmOdata;
    transient boolean isAttached;

    public SfVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String vlmOdataRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlmOdata = vlmOdataRef;
        vlmDfn = Objects.requireNonNull(vlmDfnRef);
        unmodStates = Collections.unmodifiableList(states);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public String getVlmOdata()
    {
        return vlmOdata;
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    public long getAllocatedSize()
    {
        return size;
    }

    @Override
    public long getUsableSize()
    {
        return size;
    }

    @Override
    public boolean isAttached()
    {
        return isAttached;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public VolumeDefinition getVolumeDefinition()
    {
        return vlmDfn;
    }
}
