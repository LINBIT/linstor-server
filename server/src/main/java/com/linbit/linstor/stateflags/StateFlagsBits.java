package com.linbit.linstor.stateflags;

import com.linbit.ErrorCheck;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.AbsTransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.HashSet;
import java.util.Set;

/**
 * State flags for linstor core objects
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class StateFlagsBits<PRIMARY_KEY, FLAG extends Flags> extends AbsTransactionObject
    implements StateFlags<FLAG>
{
    private final PRIMARY_KEY pk;

    private long stateFlags;
    private long changedStateFlags;
    private final long mask;
    private final StateFlagsPersistence<PRIMARY_KEY> persistence;

    public StateFlagsBits(
        final PRIMARY_KEY parent,
        final long validFlagsMask,
        final StateFlagsPersistence<PRIMARY_KEY> persistenceRef,
        final Provider<TransactionMgr> transMgrProviderRef
    )
    {
        this(parent, validFlagsMask, persistenceRef, 0L, transMgrProviderRef);
    }

    public StateFlagsBits(
        final PRIMARY_KEY pkRef,
        final long validFlagsMask,
        final StateFlagsPersistence<PRIMARY_KEY> persistenceRef,
        final long initialFlags,
        final Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);

        ErrorCheck.ctorNotNull(StateFlagsBits.class, Object.class, pkRef);
        ErrorCheck.ctorNotNull(StateFlagsBits.class, StateFlagsPersistence.class, persistenceRef);

        pk = pkRef;
        mask = validFlagsMask;
        stateFlags = initialFlags;
        changedStateFlags = initialFlags;
        persistence = persistenceRef;
    }

    @Override
    public void enableAllFlags()
        throws DatabaseException
    {

        setFlags(changedStateFlags | mask);
    }

    @Override
    public void disableAllFlags()
        throws DatabaseException
    {

        setFlags(0L);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enableFlags(final FLAG... flags)
        throws DatabaseException
    {

        final long flagsBits = getMask(flags);
        setFlags((changedStateFlags | flagsBits) & mask);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void resetFlagsTo(FLAG... flags) throws DatabaseException
    {

        final long flagsBits = getMask(flags);
        setFlags(flagsBits & mask);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void disableFlags(final FLAG... flags)
        throws DatabaseException
    {

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags & ~flagsBits);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enableFlagsExcept(final FLAG... flags)
        throws DatabaseException
    {

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags | (mask & ~flagsBits));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void disableFlagsExcept(final FLAG... flags)
        throws DatabaseException
    {

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags & flagsBits);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSet(final FLAG... flags)
    {

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) == flagsBits;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isUnset(final FLAG... flags)
    {

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) == 0L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSomeSet(final FLAG... flags)
    {

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) != 0L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSomeUnset(final FLAG... flags)
    {

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) != flagsBits;
    }

    @Override
    public long getFlagsBits()
    {

        return changedStateFlags;
    }

    @Override
    public void commitImpl()
    {
        stateFlags = changedStateFlags;
    }

    @Override
    public void rollbackImpl()
    {
        changedStateFlags = stateFlags;
    }

    @Override
    public boolean isDirty()
    {
        return changedStateFlags != stateFlags;
    }

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        return !hasTransMgr() && isDirty();
    }

    @Override
    public long getValidFlagsBits()
    {

        return mask;
    }

    public static final long getMask(final @Nullable Flags... flags)
    {
        long bitMask = 0L;
        if (flags != null)
        {
            for (Flags curFlag : flags)
            {
                bitMask |= curFlag.getFlagValue();
            }
        }
        return bitMask;
    }

    private void setFlags(final long newFlagBits) throws DatabaseException
    {
        activateTransMgr();
        long oldFlagBits = changedStateFlags;
        // k8s only persist whole object, so we MUST assign the flag-bits BEFORE calling .persist
        changedStateFlags = newFlagBits;
        persistence.persist(pk, oldFlagBits, newFlagBits);
    }

    public static <E extends Flags> Set<E> restoreFlags(E[] values, long mask)
    {
        Set<E> restoredFlags = new HashSet<>();
        for (E flag : values)
        {
            if ((mask & flag.getFlagValue()) == flag.getFlagValue())
            {
                restoredFlags.add(flag);
            }
        }
        return restoredFlags;
    }
}
