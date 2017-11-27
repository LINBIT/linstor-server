package com.linbit.linstor.stateflags;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

/**
 * State flags for linstor core objects
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class StateFlagsBits<PRIMARY_KEY, FLAG extends Flags> implements StateFlags<FLAG>
{
    private final ObjectProtection objProt;
    private final PRIMARY_KEY pk;

    private long stateFlags;
    private long changedStateFlags;
    private final long mask;
    private final StateFlagsPersistence<PRIMARY_KEY> persistence;

    private TransactionMgr transMgr;

    private boolean initialized = false;


    public StateFlagsBits(
        final ObjectProtection objProtRef,
        final PRIMARY_KEY parent,
        final long validFlagsMask,
        final StateFlagsPersistence<PRIMARY_KEY> persistenceRef
    )
    {
        this(objProtRef, parent, validFlagsMask, persistenceRef, 0L);
    }

    public StateFlagsBits(
        final ObjectProtection objProtRef,
        final PRIMARY_KEY pkRef,
        final long validFlagsMask,
        final StateFlagsPersistence<PRIMARY_KEY> persistenceRef,
        final long initialFlags
    )
    {
        objProt = objProtRef;
        pk = pkRef;
        mask = validFlagsMask;
        stateFlags = initialFlags;
        changedStateFlags = initialFlags;
        persistence = persistenceRef;
    }

    @Override
    public void enableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        setFlags(changedStateFlags | mask);
    }

    @Override
    public void disableAllFlags(final AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        setFlags(0L);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enableFlags(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags((changedStateFlags | flagsBits) & mask);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void resetFlagsTo(AccessContext accCtx, FLAG... flags) throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(flagsBits & mask);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void disableFlags(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags & ~flagsBits);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enableFlagsExcept(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags | (mask & ~flagsBits));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void disableFlagsExcept(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException, SQLException
    {
        objProt.requireAccess(accCtx, AccessType.CHANGE);

        final long flagsBits = getMask(flags);
        setFlags(changedStateFlags & flagsBits);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSet(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) == flagsBits;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isUnset(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) == 0L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSomeSet(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) != 0L;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean isSomeUnset(final AccessContext accCtx, final FLAG... flags)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        final long flagsBits = getMask(flags);
        return (changedStateFlags & flagsBits) != flagsBits;
    }

    @Override
    public long getFlagsBits(final AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return changedStateFlags;
    }

    @Override
    public void initialized()
    {
        initialized = true;
    }

    @Override
    public boolean isInitialized()
    {
        return initialized;
    }

    @Override
    public void commit()
    {
        stateFlags = changedStateFlags;
    }

    @Override
    public void rollback()
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
    public boolean hasTransMgr()
    {
        return transMgr != null;
    }

    @Override
    public void setConnection(TransactionMgr transMgrRef) throws ImplementationError
    {
        if (!hasTransMgr() && isDirtyWithoutTransMgr())
        {
            throw new ImplementationError("setConnection was called AFTER data was manipulated", null);
        }
        if (transMgrRef != null)
        {
            transMgrRef.register(this);
        }
        transMgr = transMgrRef;
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
        if (flags != null)
        {
            for (Flags curFlag : flags)
            {
                bitMask |= curFlag.getFlagValue();
            }
        }
        return bitMask;
    }

    private void setFlags(final long bits) throws SQLException
    {
        if (initialized)
        {
            if (persistence != null)
            {
                persistence.persist(pk, bits, transMgr);
                changedStateFlags = bits;
            }
            else
            {
                String npeMsg;
                if (persistence == null)
                {
                    npeMsg = "Persistence and transMgr are null";
                }
                else
                {
                    if (persistence == null)
                    {
                        npeMsg = "Persistence is null";
                    }
                    else
                    {
                        npeMsg = "transMgr is null";
                    }
                }
                throw new ImplementationError(
                    "Initilized StateFlagsBits is missing persistence or transMgr reference. "+
                        "Maybe you forget to call .setConnection(TransMgr) ?",
                    new NullPointerException(npeMsg)
                );
            }
        }
        else
        {
            changedStateFlags = bits;
            commit();
        }
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
