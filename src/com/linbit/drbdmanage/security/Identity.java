package com.linbit.drbdmanage.security;

/**
 * Identity of a security subject (user...)
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Identity
{
    // Name of this security identity
    public final IdentityName name;

    Identity(IdentityName idName)
    {
        name = idName;
    }
}
