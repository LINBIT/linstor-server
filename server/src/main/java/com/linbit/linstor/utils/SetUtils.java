package com.linbit.linstor.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class SetUtils
{
    @SafeVarargs
    public static <T extends Comparable<T>> TreeSet<T> mergeIntoTreeSet(Set<T>... sets)
    {
        return merge(new TreeSet<>(), sets);
    }
    @SafeVarargs
    public static <T> HashSet<T> mergeIntoHashSet(Set<T>... sets)
    {
        return merge(new HashSet<>(), sets);
    }

    @SafeVarargs
    public static <ELEM, SET extends Set<ELEM>> SET merge(SET targetSet, Set<ELEM>... sets)
    {
        for (Set<ELEM> set : sets)
        {
            targetSet.addAll(set);
        }
        return targetSet;
    }

}
