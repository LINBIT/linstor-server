package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

public interface ExceptionThrowingBiConsumer<T, U, EXC extends Exception>
{
    void accept(T arg1, @Nullable U arg2) throws EXC;
}
