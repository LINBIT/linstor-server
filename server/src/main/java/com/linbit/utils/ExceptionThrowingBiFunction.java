package com.linbit.utils;

public interface ExceptionThrowingBiFunction<T1, T2, R, EXC extends Exception>
{
    R accept(T1 arg1, T2 arg2) throws EXC;
}
