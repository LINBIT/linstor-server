package com.linbit.linstor.stateflags;

import com.linbit.TransactionObject;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.sql.SQLException;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StateFlags<T extends Flags> extends TransactionObject
{
    void enableAllFlags(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void disableAllFlags(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    void enableFlags(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    void disableFlags(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    void enableFlagsExcept(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    void resetFlagsTo(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    void disableFlagsExcept(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

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
