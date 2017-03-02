package com.linbit.drbdmanage.security;

import com.linbit.drbdmanage.Controller;
import java.io.IOException;

/**
 * Initializes Controller and Satellite instances with the system's security context
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Initializer
{
    private AccessContext SYSTEM_CTX;

    public Initializer()
    {
        PrivilegeSet sysPrivs = new PrivilegeSet(
            new Privilege[]
            {
                Privilege.PRIV_SYS_ALL
            }
        );

        // Create the system's security context
        SYSTEM_CTX = new AccessContext(
            Identity.SYSTEM_ID,
            Role.SYSTEM_ROLE,
            SecurityType.SYSTEM_TYPE,
            sysPrivs
        );
    }

    public final Controller initController(String[] args)
        throws IOException
    {
        return new Controller(SYSTEM_CTX, args);
    }
}
