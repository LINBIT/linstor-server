package com.linbit.linstor.spacetracking;

import java.math.BigInteger;

public class AggregateCapacityInfo
{
    public BigInteger capacity = BigInteger.ZERO;
    public BigInteger allocated = BigInteger.ZERO;
    public BigInteger usable = BigInteger.ZERO;
    public boolean storPoolExcFlag;
}
