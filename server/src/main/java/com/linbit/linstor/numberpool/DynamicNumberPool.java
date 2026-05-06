package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;


/**
 * Wrapper for a {@link NumberPool} which allows the range to be reloaded.
 */
public interface DynamicNumberPool
{
    default void reloadRange()
    {
        try
        {
            reloadRange(null);
        }
        catch (DatabaseException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Checks the registered {@link PriorityProps} (not the given parameter) for the range, re-parses it,
     * registers the range and recalculates the effective range (ranges - blocked ranges) for future usage.
     *
     * <p>If the parameter {@code propsToUpdateRef} is non-null, it will be updated if the parsed ranges
     * changed or could be simplified. If the parsed ranges render the same as before, no update will be
     * performed</p>
     */
    void reloadRange(@Nullable Props propsToUpdateRef) throws DatabaseException, AccessDeniedException;

    default void reloadBlockedRange()
    {
        try
        {
            reloadBlockedRange(null);
        }
        catch (DatabaseException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Checks the registered {@link PriorityProps} (not the given parameter) for the blocked range, re-parses it,
     * registers the blocked range and recalculates the effective range (ranges - blocked ranges) for future usage.
     *
     * <p>If the parameter {@code propsToUpdateRef} is non-null, it will be updated if the parsed blocked ranges
     * changed or could be simplified. If the parsed blocked ranges render the same as before, no update will be
     * performed</p>
     */
    void reloadBlockedRange(@Nullable Props propsToUpdateRef) throws DatabaseException, AccessDeniedException;

    void allocate(int nr)
        throws ValueInUseException;

    boolean tryAllocate(int nr);

    int autoAllocate()
        throws ExhaustedPoolException;

    void deallocate(int nr);
}
