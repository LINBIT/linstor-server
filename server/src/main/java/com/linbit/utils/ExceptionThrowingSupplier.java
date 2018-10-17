package com.linbit.utils;

@FunctionalInterface
public interface ExceptionThrowingSupplier<R, EXC extends Exception>
{
    R supply() throws EXC;
}
