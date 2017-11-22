package com.linbit.linstor.stateflags;

import com.linbit.ImplementationError;

/**
 * Base class for StateFlags persistence implementations
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class FlagsPersistenceBase
{
    // Reference to the state flags to persist
    protected StateFlags pFlags = null;

    public void setStateFlagsRef(StateFlags flagsRef)
    {
        if (pFlags == null)
        {
            pFlags = flagsRef;
        }
        else
        {
            throw new ImplementationError(
                "Illegal duplicate initialization of the persistence implementation's StateFlags reference",
                null
            );
        }
    }
}
