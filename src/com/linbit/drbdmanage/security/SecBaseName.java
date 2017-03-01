package com.linbit.drbdmanage.security;

import com.linbit.drbdmanage.Checks;
import com.linbit.drbdmanage.GenericName;
import com.linbit.drbdmanage.InvalidNameException;

/**
 * Base class for security object names
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class SecBaseName extends GenericName
{
    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 24;

    public static final byte[] VALID_CHARS = { '_' };
    public static final byte[] VALID_INNER_CHARS = { '-' };

    public SecBaseName(String genName)
        throws InvalidNameException
    {
        super(genName);
        Checks.nameCheck(genName, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }
}
