package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.Checks;
import com.linbit.GenericName;

/**
 * Valid name of a linstor free space manager
 */
public class FreeSpaceMgrName extends GenericName
{
    private static final String RESERVED_PREFIX = "SYSTEM:";

    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    private FreeSpaceMgrName(String freeSpaceMgrNameStr, boolean isReserved) throws InvalidNameException
    {
        super(isReserved ? RESERVED_PREFIX + freeSpaceMgrNameStr : freeSpaceMgrNameStr);
        Checks.nameCheck(freeSpaceMgrNameStr, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    public static FreeSpaceMgrName createName(String fsmName) throws InvalidNameException
    {
        return new FreeSpaceMgrName(fsmName, false);
    }

    public static FreeSpaceMgrName createReservedName(String storPoolName) throws InvalidNameException
    {
        return new FreeSpaceMgrName(storPoolName, true);
    }

    public static FreeSpaceMgrName restoreName(String fsmName) throws InvalidNameException
    {
        FreeSpaceMgrName ret;
        if (!fsmName.startsWith(RESERVED_PREFIX))
        {
            ret = createName(fsmName);
        }
        else
        {
            ret = createReservedName(
                fsmName.substring(
                    RESERVED_PREFIX.length(),
                    fsmName.length()
                )
            );
        }
        return ret;
    }
}