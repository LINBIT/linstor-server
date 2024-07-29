package com.linbit.linstor.stateflags;

import com.linbit.ErrorCheck;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.AbsTransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * State flags for linstor core objects
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class StateFlagsBits<PRIMARY_KEY, FLAG extends Flags> extends AbsTransactionObject
    implements StateFlags<FLAG>
{
    private final List<ObjectProtection> objProts;
    private final PRIMARY_KEY pk;

    private long stateFlags;
    private long changedStateFlags;
    private final long mask;
    private final StateFlagsPersistence<PRIMARY_KEY> persistence;

    public StateFlagsBits(
        final List<ObjectProtection> objProtRef,
        final PRIMARY_KEY parent,
        final long validFlagsMask,
        final StateFlagsPersistence<PRIMARY_KEY> persistenceRef,
        final Provider<TransactionMgr> transMgrProviderRef
    )
    {
        this(objProtRef, parent, validFlagsMask, persistenceRef, 0L, transMgrProviderRef);
    }

    public StateFlagsBits(
        final List<ObjectProtection> objProtRef,
        final PRIMARY_KEY pkRef,
        final long validFlagsMask,
        final StateFlagsPersistence<PRIMARY_KEY> persistenceRef,
        final long initialFlags,
        final Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);

        ErrorCheck.ctorNotNull(StateFlagsBits.class, ObjectProtection.class, objProtRef);
        ErrorCheck.ctorNotNull(StateFlagsBits.class, Object.class, pkRef);
        ErrorCheck.ctorNotNull(StateFlagsBits.class, StateFlagsPersistence.class, persistenceRef);

        objProts = objProtRef;
        pk = pkRef;
        mask = validFlagsMask;
        stateFlags = initialFlags;
        changedStateFlags = initialFlags;
        persistence = persistenceRef;
    }

    private void requireAccess(AccessContext accCtx, AccessType accessType)
        throws AccessDeniedException
    {
        for (ObjectProtection objProt : objProts)
        {
            objProt.requireAccess(accCtx, accessType);
        }
    }

    @Override
    public void enableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CHANGE);

        setFlags(changedStateFlags | mask);
    }

    @Override
    public void disableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CHANGE);

        setFlags(0L);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enableFlags(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags((changedStateFlags | flagsBits) & mask);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void resetFlagsTo(AccessContext accCtx, FLAG... flags) throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(flagsBits & mask);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void disableFlags(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags & ~flagsBits);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enableFlagsExcept(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags | (mask & ~flagsBits));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void disableFlagsExcept(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags & flagsBits);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSet(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) == flagsBits;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isUnset(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) == 0L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSomeSet(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) != 0L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSomeUnset(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) != flagsBits;
    }

    @Override
    public long getFlagsBits(final AccessContext accCtx)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);

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
    public long getValidFlagsBits(final AccessContext accCtx)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);

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
