package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.SfInitiatorVlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.provider.StorageRscData;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SfInitiatorData extends BaseTransactionObject implements SfInitiatorVlmProviderObject
{
    final StorageRscData rscData;
    final Volume vlm;
    final List<State> unmodStates;
    final SfVlmDfnData vlmDfnData;

    boolean exists;
    boolean isFailed = false;
    long allocatedSize;
    long usableSize;
    String devicePath;
    Size sizeState;
    List<State> states;

    public SfInitiatorData(
        StorageRscData rscDataRef,
        Volume vlmRef,
        SfVlmDfnData sfVlmDfnDataRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        rscData = Objects.requireNonNull(rscDataRef);
        vlm = Objects.requireNonNull(vlmRef);
        vlmDfnData = Objects.requireNonNull(sfVlmDfnDataRef);
        states = new ArrayList<>();
        unmodStates = Collections.unmodifiableList(states);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    public boolean isFailed()
    {
        return isFailed;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.SWORDFISH_INITIATOR;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
    }

    @Override
    public String getDevicePath()
    {
        return devicePath;
    }

    @Override
    public Size getSizeState()
    {
        return sizeState;
    }

    @Override
    public List<? extends State> getStates()
    {
        return unmodStates;
    }

    @Override
    public VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return vlmDfnData;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }
}
