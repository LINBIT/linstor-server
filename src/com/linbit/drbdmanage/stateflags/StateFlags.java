package com.linbit.drbdmanage.stateflags;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StateFlags<T extends Flags> extends TransactionObject
{
    void enableAllFlags(final AccessContext accCtx, final Connection dbConn)
        throws AccessDeniedException, SQLException;

    void disableAllFlags(final AccessContext accCtx, final Connection dbConn)
        throws AccessDeniedException, SQLException;

    void enableFlags(final AccessContext accCtx, final Connection dbConn, final T... flags)
        throws AccessDeniedException, SQLException;

    void disableFlags(final AccessContext accCtx, final Connection dbConn, final T... flags)
        throws AccessDeniedException, SQLException;

    void enableFlagsExcept(final AccessContext accCtx, final Connection dbConn, final T... flags)
        throws AccessDeniedException, SQLException;

    void disableFlagsExcept(final AccessContext accCtx, final Connection dbConn, final T... flags)
        throws AccessDeniedException, SQLException;

    boolean isSet(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

    boolean isUnset(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

    boolean isSomeSet(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

    boolean isSomeUnset(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

    long getFlagsBits(final AccessContext accCtx)
        throws AccessDeniedException;

    long getValidFlagsBits(final AccessContext accCtx)
        throws AccessDeniedException;
}
