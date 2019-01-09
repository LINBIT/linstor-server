package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdVlmDfnObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;

public class DrbdVlmDfnData extends BaseTransactionObject implements DrbdVlmDfnObject
{
    final VolumeDefinition vlmDfn;

    final MinorNumber minorNr;
    final int peerSlots;

    public DrbdVlmDfnData(
        VolumeDefinition vlmDfnRef,
        MinorNumber minorRef,
        int peerSlotsRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        minorNr = Objects.requireNonNull(minorRef);
        peerSlots = peerSlotsRef;


        vlmDfn = Objects.requireNonNull(vlmDfnRef);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
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

    @Override
    public int getPeerSlots()
    {
        return peerSlots;
    }
}
