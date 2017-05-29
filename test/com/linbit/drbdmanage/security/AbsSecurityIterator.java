package com.linbit.drbdmanage.security;

import com.linbit.testutils.AbsIterator;

public abstract class AbsSecurityIterator <T> extends AbsIterator<T>
{
    private final AccessContext rootCtx;

    private boolean iterateSecurityLevels;
    private SecurityLevel currentSecLevel;

    public AbsSecurityIterator(Object[][] values, boolean iteraterSecurityLevels, AccessContext rootCtx, int[] skipColumns)
    {
        super(values, skipColumns);

        this.rootCtx = rootCtx;
        this.iterateSecurityLevels = iteraterSecurityLevels;

        if (iterateSecurityLevels)
        {
            currentSecLevel = SecurityLevel.NO_SECURITY;
        }
        else
        {
            currentSecLevel = SecurityLevel.get();
        }
    }

    @Override
    public boolean hasNext()
    {
        boolean hasNext = super.hasNext();
        if (!hasNext && iterateSecurityLevels)
        {
            switch (SecurityLevel.get())
            {
                case NO_SECURITY:
                    currentSecLevel = SecurityLevel.RBAC;
                    resetAllIdx();
                    hasNext = true;
                    break;
                case RBAC:
                    currentSecLevel = SecurityLevel.MAC;
                    resetAllIdx();
                    hasNext = true;
                    break;
                case MAC:
                    break;
                default:
                    break;
            }
        }
        return hasNext;
    }

    @Override
    public T next()
    {
        T next = super.next(); // super.next increments indizes, which could
        // set the new currentSecLevel
        if (iterateSecurityLevels && !SecurityLevel.get().equals(currentSecLevel))
        {
            try
            {
                SecurityLevel.set(rootCtx, currentSecLevel);
            }
            catch (AccessDeniedException e)
            {
                throw new RuntimeException("rootCtx cannot change securityLevel...", e);
            }
        }
        return next;
    }
}
