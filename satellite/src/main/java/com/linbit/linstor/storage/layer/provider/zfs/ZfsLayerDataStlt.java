package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.linstor.storage.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.storage2.layer.data.State;
import com.linbit.linstor.storage2.layer.data.ZfsLayerData;

import java.util.ArrayList;
import java.util.List;

public class ZfsLayerDataStlt implements ZfsLayerData
{
    public static final State CREATED = new State(true, true, "Created");
    public static final State FAILED = new State(false, true, "Failed");

    boolean exists = false;
    boolean failed = false;
    String zpool = null;
    String identifier = null;
    Size sizeState = null;

    List<State> states = new ArrayList<>();

    public ZfsLayerDataStlt(ZfsInfo info)
    {
        this(info.poolName, info.identifier);
    }

    public ZfsLayerDataStlt(String zPoolRef, String identifierRef)
    {
        zpool = zPoolRef;
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
    public String getZPool()
    {
        return zpool;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }
}
