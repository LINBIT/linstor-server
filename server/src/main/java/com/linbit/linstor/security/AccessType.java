package com.linbit.linstor.security;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;

/**
 * Access types
 *
 * Represents the different levels of access to an object protected by access controls
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public enum AccessType
{
    VIEW((short) 0x1),
    USE((short) 0x3),
    CHANGE((short) 0x7),
    CONTROL((short) 0xF);

    private final short accessMask;

    AccessType(short mask)
    {
        accessMask = mask;
    }

    public short getAccessMask()
    {
        return accessMask;
    }

    public boolean hasAccess(AccessType requested)
    {
        return ((requested.accessMask & this.accessMask) == requested.accessMask);
    }

    /**
     * Returns an AccessType that represents the level of access that is a subset of the
     * level of access granted by both AccessType arguments
     *
     * @param first First AccessType, or null to indicate no access
     * @param second Second AccessType, or null to indicate no access
     * @return AccessType resulting from intersecting the level of access of both arguments to the method
     */
    public static AccessType intersect(AccessType first, AccessType second)
    {
        AccessType result = null;
        if (first != null && second != null)
        {
            long mask = first.accessMask & second.accessMask;
            result = getAccessByMask(mask);
        }
        return result;
    }

    /**
     * Returns an AccessType that represents the level of access that is a combination
     * of the level of access granted by each of the AccessType arguments
     *
     * @param first First AccessType, or null to indicate no access
     * @param second Second AccessType, or null to indicate no access
     * @return AccessType resulting from combining the level of access of both arguments to the method
     */
    public static AccessType union(@Nullable AccessType first, @Nullable AccessType second)
    {
        AccessType result = null;
        if (first == null)
        {
            result = second;
        }
        else
        if (second == null)
        {
            result = first;
        }
        else
        {
            long mask = first.accessMask | second.accessMask;
            result = getAccessByMask(mask);
        }
        return result;
    }

    public static AccessType get(String name)
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
                    ),
                    name
                );
        }
        return accType;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public static AccessType get(int flag)
    {
        AccessType accType;
        switch (flag)
        {
            case 1:
                accType = VIEW;
                break;
            case 3:
                accType = USE;
                break;
            case 7:
                accType = CHANGE;
                break;
            case 15:
                accType = CONTROL;
                break;
            default:
                throw new LinStorRuntimeException(
                    String.format(
                        "The value %d requested in an AccessType lookup does not match any " +
                        "known access type values",
                        flag
                    )
                );
        }
        return accType;
    }

    /**
     * Returns the AccessType for the specified access mask value
     *
     * @param mask Access mask value
     * @return AccessType associated with the specified access mask value
     */
    private static @Nullable AccessType getAccessByMask(long mask)
    {
        AccessType result = null;
        if ((mask & CONTROL.accessMask) == CONTROL.accessMask)
        {
            result = CONTROL;
        }
        else
        if ((mask & CHANGE.accessMask) == CHANGE.accessMask)
        {
            result = CHANGE;
        }
        else
        if ((mask & USE.accessMask) == USE.accessMask)
        {
            result = USE;
        }
        else
        if ((mask & VIEW.accessMask) == VIEW.accessMask)
        {
            result = VIEW;
        }
        return result;
    }
}
