package com.linbit.drbdmanage.security;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Global security level used for drbdmanageNG object protection
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public enum SecurityLevel
{
    // No security - Object protection off
    NO_SECURITY,

    // Role based access control - Objects protected by access control lists
    RBAC,

    // Mandatory access control - Objects additionaly protected by domain/type rules
    MAC;

    private static final AtomicReference<SecurityLevel> global = new AtomicReference<>(SecurityLevel.MAC);

    public static SecurityLevel get()
    {
        return global.get();
    }

    public static void set(AccessContext accCtx, SecurityLevel newLevel)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        global.set(newLevel);
    }
}