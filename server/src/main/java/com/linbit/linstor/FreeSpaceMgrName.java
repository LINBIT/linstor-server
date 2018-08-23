package com.linbit.linstor;

import com.linbit.InvalidNameException;
import com.linbit.Checks;
import com.linbit.GenericName;

/**
 * Valid name of a linstor free space manager
 */
public class FreeSpaceMgrName extends GenericName
{
    private static final String RESERVED_CONNECTOR = ":";

    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_'};
    // FIXME: as shown below a FreeSpaceMgrName might be a concatenation of
    // NodeName and StorPoolName. This means FreeSpaceName has to allow
    // at least as much as NodeName!
    public static final byte[] VALID_INNER_CHARS = {'_', '-', '.'};

    public FreeSpaceMgrName(String freeSpaceMgrNameStr) throws InvalidNameException
    {
        super(freeSpaceMgrNameStr);
        Checks.nameCheck(freeSpaceMgrNameStr, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    public FreeSpaceMgrName(NodeName nodeName, StorPoolName storPoolName)
    {
        super(nodeName.displayValue + RESERVED_CONNECTOR + storPoolName.displayValue);
    }

    public static FreeSpaceMgrName restoreName(String fsmName) throws InvalidNameException
    {
        FreeSpaceMgrName ret;
        if (!fsmName.contains(RESERVED_CONNECTOR))
        {
            ret = new FreeSpaceMgrName(fsmName);
        }
        else
        {
            String[] parts = fsmName.split(RESERVED_CONNECTOR);
            if (parts.length != 2)
            {
                throw new InvalidNameException("Reserved name has incorrect number of parts", fsmName);
            }
            ret = new FreeSpaceMgrName(new NodeName(parts[0]), new StorPoolName(parts[1]));
        }
        return ret;
    }
}
