package com.linbit.linstor.security;

import com.linbit.InvalidNameException;

/**
 * Name of a security role
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class RoleName extends SecBaseName
{
    public RoleName(String genName) throws InvalidNameException
    {
        super(genName);
    }
}
