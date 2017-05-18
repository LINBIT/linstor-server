package com.linbit.drbdmanage.stateflags;

import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;

/**
 * State flags for drbdmanage core objects
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class StateFlagsBits<T extends Flags> implements StateFlags<T>
{
    private final ObjectProtection objProt;

    private long stateFlags;
    private final long mask;

    public StateFlagsBits(final ObjectProtection objProtRef, final long validFlagsMask)
    {
        this(objProtRef, validFlagsMask, 0L);
    }

    public StateFlagsBits(final ObjectProtection objProtRef, final long validFlagsMask, final long initialFlags)
    {
        objProt = objProtRef;
        mask = validFlagsMask;
        stateFlags = initialFlags;
    }

    @Override
    public void enableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        stateFlags |= mask;
    }

    @Override
    public void disableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        stateFlags = 0L;
    }

    @Override
    public void enableFlags(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        stateFlags = (stateFlags | flagsBits) & mask;
    }

    @Override
    public void disableFlags(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        stateFlags &= ~flagsBits;
    }

    @Override
    public void enableFlagsExcept(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        stateFlags |= (mask & ~flagsBits);
    }

    @Override
    public void disableFlagsExcept(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        stateFlags &= flagsBits;
    }

    @Override
    public boolean isSet(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (stateFlags & flagsBits) == flagsBits;
    }

    @Override
    public boolean isUnset(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (stateFlags & flagsBits) == 0L;
    }

    @Override
    public boolean isSomeSet(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (stateFlags & flagsBits) != 0L;
    }

    @Override
    public boolean isSomeUnset(final AccessContext accCtx, final T... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (stateFlags & flagsBits) != flagsBits;
    }

    @Override
    public long getFlagsBits(final AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return stateFlags;
    }

    @Override
    public long getValidFlagsBits(final AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return mask;
    }

    public static final long getMask(final Flags... flags)
    {
        long bitMask = 0L;
        for (Flags curFlag : flags)
        {
            bitMask |= curFlag.getFlagValue();
        }
        return bitMask;
    }
}
