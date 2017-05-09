package com.linbit.drbdmanage.security;

import com.linbit.InvalidNameException;

/**
 * Access types
 *
 * Represents the different levels of access to an object protected by access controls
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public enum AccessType
{
    VIEW(0x1L),
    USE(0x3L),
    CHANGE(0x7L),
    CONTROL(0xFL);

    private final long accessMask;

    private AccessType(long mask)
    {
        accessMask = mask;
    }

    public final boolean hasAccess(AccessType requested)
    {
        return ((requested.accessMask & this.accessMask) == requested.accessMask);
    }

    public static final AccessType get(String name)
        throws InvalidNameException
    {
        AccessType accType;
        String upperName = name.toUpperCase();
        switch (upperName)
        {
            case "VIEW":
                accType = VIEW;
                break;
            case "USE":
                accType = USE;
                break;
            case "CHANGE":
                accType = CHANGE;
                break;
            case "CONTROL":
                accType = CONTROL;
                break;
            default:
                throw new InvalidNameException(
                    String.format(
                        "The name '%s' requested in an AccessType lookup does not match any " +
                        "known access type names",
                        upperName
                    )
                );
        }
        return accType;
    }
}
