package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.LvmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.provider.StorageRscData;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class LvmData extends BaseTransactionObject implements LvmProviderObject
{
    private final List<? extends State> unmodStates;

    final Volume vlm;
    final StorageRscData rscData;

    transient boolean exists;
    transient boolean failed;
    transient long allocatedSize;
    transient long usableSize;
    transient String devicePath;
    transient String volumeGroup;
    transient String identifier;
    transient Size sizeState = null;

    final transient List<? extends State> states;

    public LvmData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);

        states = new ArrayList<>();
        unmodStates = Collections.unmodifiableList(states);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    void updateInfo(LvsInfo info)
    {
        exists = true;
        volumeGroup = info.volumeGroup;
        devicePath = info.path;
        identifier = info.identifier;
    }

    @Override
    public Size getSizeState()
    {
        return sizeState;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.LVM;
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
    public List<? extends State> getStates()
    {
        return unmodStates;
    }

    @Override
    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }
}
