package com.linbit.utils;

@FunctionalInterface
public interface ExceptionThrowingProducer<R, EXC extends Exception>
{
    R produce() throws EXC;
}
