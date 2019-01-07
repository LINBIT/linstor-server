package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.linstor.storage.layer.data.LvmLayerData;
import com.linbit.linstor.storage.layer.data.State;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;

import java.util.ArrayList;
import java.util.List;

public class LvmLayerDataStlt implements LvmLayerData
{
    public static final State CREATED = new State(true, true, "Created");
    public static final State FAILED = new State(false, true, "Failed");

    boolean exists = false;
    boolean failed = false;
    String volumeGroup = null;
    String thinPool;
    String identifier = null;
    Size sizeState = null;
    List<State> states = new ArrayList<>();

    public LvmLayerDataStlt(LvsInfo info)
    {
        this(info.volumeGroup, info.thinPool, info.identifier);
    }

    public LvmLayerDataStlt(String volumeGroupRef, String thinPoolRef, String identifierRef)
    {
        volumeGroup = volumeGroupRef;
        thinPool = thinPoolRef;
        identifier = identifierRef;
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
    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }

    @Override
    public List<State> getStates()
    {
        return states;
    }
}
