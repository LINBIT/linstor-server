package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

import java.util.Objects;

public class Pair<A, B> implements Comparable<Pair<A, B>>
{
    public @Nullable A objA;
    public @Nullable B objB;

    public Pair()
    {
    }

    public Pair(@Nullable A aRef, @Nullable B bRef)
    {
        objA = aRef;
        objB = bRef;
    }

    @Override
    public int hashCode()
    {
        return hashCode(objA, objB);
    }

    static int hashCode(@Nullable Object objA, @Nullable Object objB)
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
            eq = Objects.equals(objA, other.objA) && Objects.equals(objB, other.objB);
        }
        return eq;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Pair<A, B> otherPair)
    {
        int eq = 0;
        if (objA == null)
        {
            eq = otherPair.objA == null ? 0 : -1;
        }
        else
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

    @Override
    public String toString()
    {
        return "Pair (" + java.util.Objects.toString(objA) + ", " + java.util.Objects.toString(objB) + ")";
    }
}
