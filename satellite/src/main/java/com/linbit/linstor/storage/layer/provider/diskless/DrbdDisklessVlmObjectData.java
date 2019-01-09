package com.linbit.linstor.storage.layer.provider.diskless;

import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DrbdDisklessVlmObjectData extends BaseTransactionObject implements VlmProviderObject
{
    final Volume vlm;
    final RscLayerObject rscData;
    final List<? extends State> unmodStates;

    final transient boolean exists = true;
    final transient boolean failed = false;
    final transient List<? extends State> states;

    public DrbdDisklessVlmObjectData(
        Volume vlmRef,
        RscLayerObject rscDataRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        rscData = rscDataRef;
        vlm = Objects.requireNonNull(vlmRef);

        states = new ArrayList<>();
        unmodStates = Collections.unmodifiableList(states);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.DRBD_DISKLESS;
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    @Override
    public long getAllocatedSize()
    {
        return 0;
    }

    @Override
    public long getUsableSize()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public String getDevicePath()
    {
        return "none";
    }

    @Override
    public Size getSizeState()
    {
        return Size.AS_EXPECTED;
    }

    @Override
    public List<? extends State> getStates()
    {
        return unmodStates;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }
}
