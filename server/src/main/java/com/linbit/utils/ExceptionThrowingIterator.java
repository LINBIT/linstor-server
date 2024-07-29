package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

public interface ExceptionThrowingIterator<E, EXC extends Exception>
{
    boolean hasNext() throws EXC;

    @Nullable
    E next() throws EXC;
}
