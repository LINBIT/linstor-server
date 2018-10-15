package com.linbit.utils;

import java.util.Comparator;
import java.util.function.Function;

public class ComparatorUtils
{
    public static <T, U> Comparator<T> comparingWithComparator(Function<T, U> mapper, Comparator<U> comparator)
    {
        return (t1, t2) -> comparator.compare(mapper.apply(t1), mapper.apply(t2));
    }

    private ComparatorUtils()
    {
    }
}
