package com.linbit.utils;

import com.google.common.base.Objects;

public class Tuple<A, B> implements Comparable<Tuple<A, B>>
{
    public A objA;
    public B objB;

    public Tuple()
    {
    }

    public Tuple(A aRef, B bRef)
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
            Tuple<?, ?> other = (Tuple<?, ?>) obj;
            eq = Objects.equal(objA, other.objA) && Objects.equal(objB, other.objB);
        }
        return eq;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Tuple<A, B> otherTuple)
    {
        int eq = 0;
        if (objA instanceof Comparable)
        {
            eq = ((Comparable<A>) objA).compareTo(otherTuple.objA);
        }
        if (eq == 0 && objB instanceof Comparable)
        {
            eq = ((Comparable<B>) objB).compareTo(otherTuple.objB);
        }
        return eq;
    }
}
