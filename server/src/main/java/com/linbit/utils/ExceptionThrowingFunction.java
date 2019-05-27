package com.linbit.utils;

public interface ExceptionThrowingFunction<T, R, EXC extends Exception>
{
    R accept(T arg) throws EXC;
}
