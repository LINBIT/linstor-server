package com.linbit.utils;

import com.google.common.base.Objects;

public class Pair<A, B> implements Comparable<Pair<A, B>>
{
    public A objA;
    public B objB;

    public Pair()
    {
    }

    public Pair(A aRef, B bRef)
    {
        objA = aRef;
        objB = bRef;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((objA == null) ? 0 : objA.hashCode());
        result = prime * result + ((objB == null) ? 0 : objB.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean eq = this == obj;
        if (!eq && obj != null && getClass() == obj.getClass())
        {
            Pair<?, ?> other = (Pair<?, ?>) obj;
            eq = Objects.equal(objA, other.objA) && Objects.equal(objB, other.objB);
        }
        return eq;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Pair<A, B> otherPair)
    {
        int eq = 0;
        if (objA instanceof Comparable)
        {
            eq = ((Comparable<A>) objA).compareTo(otherPair.objA);
        }
        if (eq == 0 && objB instanceof Comparable)
        {
            eq = ((Comparable<B>) objB).compareTo(otherPair.objB);
        }
        return eq;
    }
}
