package com.linbit.utils;

public interface BiExceptionThrowingBiConsumer<T, U, EXC1 extends Exception, EXC2 extends Exception>
{
    void accept(T arg1, U arg2) throws EXC1, EXC2;
}
