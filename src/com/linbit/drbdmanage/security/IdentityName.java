package com.linbit.drbdmanage.security;

import com.linbit.drbdmanage.InvalidNameException;

/**
 * Name of a security identity
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class IdentityName extends SecBaseName
{
    public IdentityName(String genName) throws InvalidNameException
    {
        super(genName);
    }
}
