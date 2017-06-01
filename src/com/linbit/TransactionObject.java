package com.linbit;

/**
 * Interface for objects that can apply or undo one or multiple previously performed changes
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface TransactionObject
{
    void commit();
    void rollback();
}
