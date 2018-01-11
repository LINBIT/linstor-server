package com.linbit.linstor.security;

public class DummySecurityInitializer
{
    public static AccessContext getSystemAccessContext()
    {
        PrivilegeSet sysPrivs = new PrivilegeSet(Privilege.PRIV_SYS_ALL);

        AccessContext sysCtx = new AccessContext(
            Identity.SYSTEM_ID,
            Role.SYSTEM_ROLE,
            SecurityType.SYSTEM_TYPE,
            sysPrivs
        );
        try
        {
            sysCtx.privEffective.enablePrivileges(Privilege.PRIV_SYS_ALL);
            return sysCtx;
        }
        catch (AccessDeniedException iAmNotRootExc)
        {
            throw new RuntimeException(iAmNotRootExc);
        }
    }

    public static AccessContext getPublicAccessContext()
    {
        PrivilegeSet pubPrivs = new PrivilegeSet();

        AccessContext pubCtx = new AccessContext(
            Identity.PUBLIC_ID,
            Role.PUBLIC_ROLE,
            SecurityType.PUBLIC_TYPE,
            pubPrivs
        );
        return pubCtx;
    }

    public static ObjectProtection getDummyObjectProtection(AccessContext accCtx)
    {
        return new ObjectProtection(accCtx, null, null);
    }
}
