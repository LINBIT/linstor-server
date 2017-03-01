package com.linbit.drbdmanage.security;

/**
 * Access types
 *
 * Represents the different levels of access to an object protected by access controls
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public enum AccessType
{
    VIEW(0x1L),
    USE(0x3L),
    CHANGE(0x7L),
    CONTROL(0xFL);

    private final long accessMask;

    private AccessType(long mask)
    {
        accessMask = mask;
    }

    public final boolean hasAccess(AccessType requested)
    {
        return ((requested.accessMask & this.accessMask) == requested.accessMask);
    }
}
