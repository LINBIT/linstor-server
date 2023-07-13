package com.linbit.linstor.spacetracking;

import com.linbit.linstor.core.identifier.NodeName;

import java.math.BigInteger;

public class AggregateCapacityInfo
{
    public final NodeName nodeName;

    public BigInteger capacity = BigInteger.ZERO;
    public BigInteger allocated = BigInteger.ZERO;
    public BigInteger usable = BigInteger.ZERO;
    public boolean storPoolExcFlag;

    public AggregateCapacityInfo(NodeName nodeNameRef)
    {
        nodeName = nodeNameRef;
    }
}
