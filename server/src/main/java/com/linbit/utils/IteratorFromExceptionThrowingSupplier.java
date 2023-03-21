package com.linbit.utils;

public class IteratorFromExceptionThrowingSupplier<E, EXC extends Exception>
{
    private final ExceptionThrowingSupplier<E, EXC> supplier;
    private E next;
    private boolean supplied;

    public IteratorFromExceptionThrowingSupplier(ExceptionThrowingSupplier<E, EXC> supplierRef) throws EXC
    {
        supplier = supplierRef;
        supplied = false;
    }

    public boolean prepareNext() throws EXC
    {
        if (!supplied)
        {
            next = supplier.supply();
            supplied = true;
        }
        return next != null;
    }

    public E getNext()
    {
        supplied = false;
        return next;
    }
}
