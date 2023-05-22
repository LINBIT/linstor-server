package com.linbit.utils;

public interface ExceptionThrowingIterator<E, EXC extends Exception>
{
    boolean hasNext() throws EXC;

    E next() throws EXC;
}
