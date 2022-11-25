package com.linbit.utils;

public class IteratorFromExceptionThrowingSupplier<E, EXC extends Exception>
{
    private final ExceptionThrowingSupplier<E, EXC> supplier;
    private E next;

    public IteratorFromExceptionThrowingSupplier(ExceptionThrowingSupplier<E, EXC> supplierRef) throws EXC
    {
        supplier = supplierRef;
        next = supplier.supply();
    }

    public boolean hasNext()
    {
        return next != null;
    }

    public E next() throws EXC
    {
        E ret = next;
        next = supplier.supply();
        return ret;
    }
}
