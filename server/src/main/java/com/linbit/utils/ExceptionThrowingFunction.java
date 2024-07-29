package com.linbit.utils;

import com.linbit.linstor.annotation.Nullable;

public interface ExceptionThrowingFunction<T, R, EXC extends Exception>
{
    @Nullable
    R accept(T arg) throws EXC;
}
