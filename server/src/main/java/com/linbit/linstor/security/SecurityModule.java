package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PublicContext;
import com.linbit.linstor.annotation.SystemContext;

public class SecurityModule extends AbstractModule
{
    private static final AccessContext SYSTEM_CTX;
    private static final AccessContext PUBLIC_CTX;

    static
    {
        PrivilegeSet sysPrivs = new PrivilegeSet(Privilege.PRIV_SYS_ALL);

        // Create the system's security context
        SYSTEM_CTX = new AccessContext(
            Identity.SYSTEM_ID,
            Role.SYSTEM_ROLE,
            SecurityType.SYSTEM_TYPE,
            sysPrivs
        );

        PrivilegeSet publicPrivs = new PrivilegeSet();

        PUBLIC_CTX = new AccessContext(
            Identity.PUBLIC_ID,
            Role.PUBLIC_ROLE,
            SecurityType.PUBLIC_TYPE,
            publicPrivs
        );
    }

    public SecurityModule()
    {
        try
        {
            AccessContext initCtx = SYSTEM_CTX.clone();
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            // Adjust the type enforcement rules for the SYSTEM domain/type
            SecurityType.SYSTEM_TYPE.addRule(
                initCtx,
                SecurityType.SYSTEM_TYPE, AccessType.CONTROL
            );
        }
        catch (AccessDeniedException accessExc)
        {
            throw new ImplementationError(
                "The built-in SYSTEM security context has insufficient privileges " +
                    "to initialize the security subsystem.",
                accessExc
            );
        }
    }

    @Override
    protected void configure()
    {
        AccessContext initCtx = SYSTEM_CTX.clone();
        try
        {
            initCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
        }
        catch (AccessDeniedException accessExc)
        {
            throw new ImplementationError(
                "The built-in SYSTEM security context has insufficient privileges " +
                    "to initialize the security subsystem.",
                accessExc
            );
        }

        bind(AccessContext.class).annotatedWith(SystemContext.class).toInstance(initCtx);
        bind(AccessContext.class).annotatedWith(PublicContext.class).toInstance(PUBLIC_CTX);
    }
}
