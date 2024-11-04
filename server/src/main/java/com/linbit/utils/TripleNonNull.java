package com.linbit.utils;

import com.google.common.base.Objects;

public class TripleNonNull<A, B, C> implements Comparable<TripleNonNull<A, B, C>>
{
    public A objA;
    public B objB;
    public C objC;

    public TripleNonNull(A aRef, B bRef, C cRef)
    {
        objA = aRef;
        objB = bRef;
        objC = cRef;
    }

    @Override
    public int hashCode()
    {
        return Triple.hashCode(objA, objB, objC);
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean eq = this == obj;
        if (!eq && obj != null && getClass() == obj.getClass())
        {
            TripleNonNull<?, ?, ?> other = (TripleNonNull<?, ?, ?>) obj;
            eq = Objects.equal(objA, other.objA) &&
                Objects.equal(objB, other.objB) &&
                Objects.equal(objC, other.objC);
        }
        return eq;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(TripleNonNull<A, B, C> other)
    {
        int eq = 0;
        if (objA instanceof Comparable)
        {
            eq = ((Comparable<A>) objA).compareTo(other.objA);
        }
        if (eq == 0 && objB instanceof Comparable)
        {
            eq = ((Comparable<B>) objB).compareTo(other.objB);
        }
        if (eq == 0 && objC instanceof Comparable)
        {
            eq = ((Comparable<C>) objC).compareTo(other.objC);
        }
        return eq;
    }
}
