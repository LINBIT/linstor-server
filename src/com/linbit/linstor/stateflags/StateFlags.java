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
    public void enableAllFlags(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public void disableAllFlags(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public void enableFlags(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    public void disableFlags(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    public void enableFlagsExcept(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    public void resetFlagsTo(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    public void disableFlagsExcept(AccessContext accCtx, T... flags)
        throws AccessDeniedException, SQLException;

    public boolean isSet(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    public boolean isUnset(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    public boolean isSomeSet(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    public boolean isSomeUnset(AccessContext accCtx, T... flags)
        throws AccessDeniedException;

    public long getFlagsBits(AccessContext accCtx)
        throws AccessDeniedException;

    public long getValidFlagsBits(AccessContext accCtx)
        throws AccessDeniedException;

}
