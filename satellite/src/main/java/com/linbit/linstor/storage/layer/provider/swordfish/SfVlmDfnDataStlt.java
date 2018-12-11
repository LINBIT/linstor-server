package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.linstor.storage2.layer.data.SfVlmDfnLayerData;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SfVlmDfnDataStlt extends BaseTransactionObject implements SfVlmDfnLayerData
{

    public enum State
    {
        FAILED
    }

    boolean exists;
    List<State> states = new ArrayList<>();
    long size;
    String vlmOdata;
    boolean isAttached;

    public SfVlmDfnDataStlt(
        Provider<TransactionMgr> transMgrProvider,
        boolean existsRef,
        List<State> statesRef,
        long sizeRef,
        String vlmOdataRef,
        boolean isAttachedRef
    )
    {
        this(transMgrProvider);
        exists = existsRef;
        states = statesRef;
        size = sizeRef;
        vlmOdata = vlmOdataRef;
        isAttached = isAttachedRef;
    }

    public SfVlmDfnDataStlt(Provider<TransactionMgr> transMgrProvider)
    {
        super(transMgrProvider);
        transObjs = Collections.emptyList();
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
}
