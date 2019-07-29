package com.linbit.linstor.stateflags;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StateFlags<T extends Flags> extends TransactionObject
{
    void enableAllFlags(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    void disableAllFlags(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    void enableFlags(AccessContext accCtx, T... flags)
        throws AccessDeniedException, DatabaseException;

    void disableFlags(AccessContext accCtx, T... flags)
        throws AccessDeniedException, DatabaseException;

    void enableFlagsExcept(AccessContext accCtx, T... flags)
        throws AccessDeniedException, DatabaseException;

    void resetFlagsTo(AccessContext accCtx, T... flags)
        throws AccessDeniedException, DatabaseException;

    void disableFlagsExcept(AccessContext accCtx, T... flags)
        throws AccessDeniedException, DatabaseException;

    boolean isSet(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    boolean isUnset(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    boolean isSomeSet(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    boolean isSomeUnset(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    long getFlagsBits(AccessContext accCtx)
        throws AccessDeniedException;

    long getValidFlagsBits(AccessContext accCtx)
        throws AccessDeniedException;

}
