package com.linbit.utils;

public interface ExceptionThrowingBiConsumer<T, U, EXC extends Exception>
{
    void accept(T arg1, U arg2) throws EXC;
}
