package com.linbit.utils;

public interface ExceptionThrowingFunction<T, R, EXC extends Exception>
{
    R apply(T arg) throws EXC;
}
