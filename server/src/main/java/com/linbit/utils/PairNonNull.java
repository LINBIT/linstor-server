package com.linbit.utils;

import java.util.Objects;

public class PairNonNull<A, B> implements Comparable<PairNonNull<A, B>>
{
    // TODO: remove this class and use pair instead as soon as internal issue #1217 is done
    public A objA;
    public B objB;

    public PairNonNull(A objARef, B objBRef)
    {
        objA = objARef;
        objB = objBRef;
    }

    @Override
    public int hashCode()
    {
        return Pair.hashCode(objA, objB);
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean eq = this == obj;
        if (!eq && obj != null && getClass() == obj.getClass())
        {
            PairNonNull<?, ?> other = (PairNonNull<?, ?>) obj;
            eq = Objects.equals(objA, other.objA) && Objects.equals(objB, other.objB);
        }
        return eq;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(PairNonNull<A, B> otherPair)
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
