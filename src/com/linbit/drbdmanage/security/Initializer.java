package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.InvalidNameException;
import java.io.IOException;

/**
 * Initializes Controller and Satellite instances with the system's security context
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Initializer
{
    static final Identity   SYSTEM_ID;
    private Role            SYSTEM_ROLE;
    private SecurityType    SYSTEM_SEC_DOMAIN;
    private AccessContext   SYSTEM_CTX;

    static
    {
        try
        {
            IdentityName sysIdName = new IdentityName("SYSTEM");
            SYSTEM_ID = new Identity(sysIdName);
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of one of the system's security objects contains an invalid name",
                nameExc
            );
        }
    };

    public Initializer()
    {
        try
        {
            RoleName sysRoleName = new RoleName("SYSTEM");
            SYSTEM_ROLE = new Role(sysRoleName);

            SecTypeName sysSecDomName = new SecTypeName("SYSTEM");
            SYSTEM_SEC_DOMAIN = new SecurityType(sysSecDomName);

            PrivilegeSet sysPrivs = new PrivilegeSet(
                new Privilege[]
                {
                    Privilege.PRIV_SYS_ALL
                }
            );

            // Create the system's security context
            SYSTEM_CTX = new AccessContext(SYSTEM_ID, SYSTEM_ROLE, SYSTEM_SEC_DOMAIN, sysPrivs);
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of one of the system's security objects contains an invalid name",
                nameExc
            );
        }
    }

    public final Controller initController(String[] args)
        throws IOException
    {
        return new Controller(SYSTEM_CTX, args);
    }
}
