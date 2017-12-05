package com.linbit.linstor.security;

public class TestAccessContextProvider
{
    public static final AccessContext sysCtx;

    static
    {
        PrivilegeSet sysPrivs = new PrivilegeSet(Privilege.PRIV_SYS_ALL);
    
        sysCtx = new AccessContext(
            Identity.SYSTEM_ID,
            Role.SYSTEM_ROLE,
            SecurityType.SYSTEM_TYPE,
            sysPrivs
        );
        try
        {
            sysCtx.privEffective.enablePrivileges(Privilege.PRIV_SYS_ALL);
        }
        catch (AccessDeniedException iAmNotRootExc)
        {
            throw new RuntimeException(iAmNotRootExc);
        }
    }
}

