package com.linbit.utils;

public interface ExceptionThrowingIterator<E, EXC extends Exception>
{
    boolean hasNext();

    E next() throws EXC;
}
