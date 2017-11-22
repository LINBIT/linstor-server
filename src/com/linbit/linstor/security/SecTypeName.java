package com.linbit.linstor.security;

import com.linbit.InvalidNameException;

/**
 * Name of a security type
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class SecTypeName extends SecBaseName
{
    public SecTypeName(String genName) throws InvalidNameException
    {
        super(genName);
    }
}
