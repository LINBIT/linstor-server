package com.linbit.drbdmanage.stateflags;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StateFlags<T extends Flags>
{
    void enableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException;

    void disableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException;

    void enableFlags(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

    void disableFlags(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

    void enableFlagsExcept(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

    void disableFlagsExcept(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException;

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
