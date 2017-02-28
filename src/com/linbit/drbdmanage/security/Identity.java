package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.InvalidNameException;

/**
 * Identity of a security subject (user...)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Identity
{
    // Name of this security identity
    public final IdentityName name;

    static final Identity SYSTEM_ID;

    static
    {
        try
        {
            SYSTEM_ID = new Identity(new IdentityName("SYSTEM"));
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The name constant of the system identity is invalid",
                nameExc
            );
        }
    }

    Identity(IdentityName idName)
    {
        name = idName;
    }
}
