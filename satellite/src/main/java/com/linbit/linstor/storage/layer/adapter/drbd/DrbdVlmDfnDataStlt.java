package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.storage.layer.data.DrbdVlmDfnData;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;

public class DrbdVlmDfnDataStlt extends BaseTransactionObject implements DrbdVlmDfnData
{
    // data from controller - read only
    final MinorNumber minor;
    final int peerSlots;

    // temporary data - satellite only

    public DrbdVlmDfnDataStlt(
        MinorNumber minorRef,
        int peerSlotsRef,
        Provider<TransactionMgr> transMgrProvider)
    {
        super(transMgrProvider);
        minor = minorRef;
        peerSlots = peerSlotsRef;

        transObjs = Arrays.asList();
    }

    @Override
    public MinorNumber getMinorNr()
    {
        return minor;
    }

    @Override
    public int getPeerSlots()
    {
        return peerSlots;
    }
}
