package com.linbit.utils;

public interface ExceptionThrowingConsumer<T, EXC extends Exception>
{
    void accept(T obj) throws EXC;
}
