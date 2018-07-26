package com.linbit.timer;

/**
 * An Action is a Runnable that is identifiable by a unique id of the specified type
 *
 * @param <K> Type of the unique identifier for Action instances
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Action<K extends Comparable<K>> extends Runnable
{
    /**
     * Returns the unique identifier of this Action instance
     *
     * @return unique identifier of this Action instance
     */
    K getId();
}
