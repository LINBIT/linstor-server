package com.linbit.linstor.spacetracking;

import com.linbit.linstor.core.identifier.NodeName;

import java.math.BigInteger;
import java.util.Objects;

public class AggregateCapacityInfo implements Comparable<AggregateCapacityInfo>
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

    @Override
    public int compareTo(AggregateCapacityInfo oRef)
    {
        int cmp;
        if (nodeName == null)
        {
            if (oRef.nodeName == null)
            {
                cmp = 0;
            }
            else
            {
                cmp = -1;
            }
        }
        else
        {
            if (oRef.nodeName == null)
            {
                cmp = 1;
            }
            else
            {
                cmp = nodeName.compareTo(oRef.nodeName);
            }
        }
        return cmp;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof AggregateCapacityInfo))
        {
            return false;
        }
        AggregateCapacityInfo other = (AggregateCapacityInfo) obj;
        return Objects.equals(nodeName, other.nodeName);
    }
}
