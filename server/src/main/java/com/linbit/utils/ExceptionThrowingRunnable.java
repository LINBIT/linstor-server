package com.linbit.utils;

public interface ExceptionThrowingRunnable<EXC extends Exception>
{
    void run() throws EXC;
}
