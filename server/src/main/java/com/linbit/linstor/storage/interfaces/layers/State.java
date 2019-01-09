package com.linbit.linstor.storage.interfaces.layers;

public class State
{
    private final boolean good;
    private final boolean stable;
    private final String descr;

    public State(boolean goodRef, boolean stableRef, String descrRef)
    {
        good = goodRef;
        stable = stableRef;
        descr = descrRef;
    }

    public boolean isGoodState()
    {
        return good;
    }

    public boolean isStable()
    {
        return stable;
    }

    @Override
    public String toString()
    {
        return descr;
    }
}
