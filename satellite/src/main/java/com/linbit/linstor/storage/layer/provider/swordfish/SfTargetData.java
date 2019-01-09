package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.SfTargetVlmProviderObject;
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

public class SfTargetData extends BaseTransactionObject implements SfTargetVlmProviderObject
{
    private static final int ZERO_USABLE_SIZE = 0; // target is never usable, only initiator
    private static final String DEV_NULL = "/dev/null"; // target is never usable, only initiator

    final Volume vlm;
    final StorageRscData rscData;
    final SfVlmDfnData vlmDfnData;
    final List<State> unmodStates;

    boolean isFailed = false;
    Size sizeState;
    transient List<State> states;
    long allocatedSize;
    String storPoolService;

    public SfTargetData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        SfVlmDfnData vlmDfnDataRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlmDfnData = Objects.requireNonNull(vlmDfnDataRef);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);

        states = new ArrayList<>();
        unmodStates = Collections.unmodifiableList(states);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public boolean exists()
    {
        return vlmDfnData.exists;
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
        return DeviceProviderKind.SWORDFISH_TARGET;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    @Override
    public long getUsableSize()
    {
        return ZERO_USABLE_SIZE;
    }

    @Override
    public String getDevicePath()
    {
        return DEV_NULL;
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
    public String getVlmOdata()
    {
        return vlmDfnData.vlmOdata;
    }

    @Override
    public String getStorPoolService()
    {
        return storPoolService;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }
    @Override
    public VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return vlmDfnData;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }
}
