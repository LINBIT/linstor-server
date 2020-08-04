package com.linbit.utils;

public interface ExceptionThrowingPredicate<T, EXC extends Exception>
{
    boolean test(T arg) throws EXC;
}
