package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DrbdVlmData extends BaseTransactionObject implements DrbdVlmObject
{
    final Volume vlm;
    final DrbdRscData rscData;
    final DrbdVlmDfnData vlmDfnData;
    final List<? extends State> unmodStates;

    boolean exists;
    boolean failed;
    String metaDiskPath;
    long allocatedSize;
    long usableSize;
    String devicePath;
    String diskState;
    Size sizeState;

    transient short peerSlots;
    transient int alStripes;
    transient long alStripeSize;
    transient boolean hasMetaData;
    transient boolean checkMetaData;
    transient boolean metaDataIsNew;
    transient boolean hasDisk;
    transient List<? extends State> states;


    public DrbdVlmData(
        Volume vlmRef,
        DrbdRscData rscDataRef,
        DrbdVlmDfnData vlmDfnDataRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);

        vlm = vlmRef;
        rscData = rscDataRef;
        vlmDfnData = vlmDfnDataRef;

        exists = false;
        failed = false;
        metaDiskPath = null;

        peerSlots = InternalApiConsts.DEFAULT_PEER_SLOTS;
        alStripes = -1;
        alStripeSize = -1L;

        checkMetaData = true;
        metaDataIsNew = false;

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
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
    }

    @Override
    public String getMetaDiskPath()
    {
        return metaDiskPath;
    }

    @Override
    public String getDiskState()
    {
        return diskState;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public DrbdVlmDfnData getVlmDfnLayerObject()
    {
        return vlmDfnData;
    }

    @Override
    public DrbdRscData getRscLayerObject()
    {
        return rscData;
    }
}
