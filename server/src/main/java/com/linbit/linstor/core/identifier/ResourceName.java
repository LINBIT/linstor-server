package com.linbit.linstor.core.identifier;

import com.linbit.InvalidNameException;
import com.linbit.Checks;
import com.linbit.GenericName;

/**
 * Valid name of a linstor resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceName extends GenericName
{
    public static final int MIN_LENGTH = 2;
    public static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public static final String RESERVED_KEYWORD_ALL = "all";
    public static final String RESERVED_KEYWORD_PROXY = "proxy";

    public ResourceName(String resName) throws InvalidNameException
    {
        super(resName);
        if (resName.equalsIgnoreCase(RESERVED_KEYWORD_ALL) || resName.equalsIgnoreCase(RESERVED_KEYWORD_PROXY))
        {
            throw new InvalidNameException(
                "The specified name '" + resName +
                "' cannot be used because it matches a keyword reserved for DRBD.",
                resName
            );
        }
        Checks.nameCheck(resName, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }
}
