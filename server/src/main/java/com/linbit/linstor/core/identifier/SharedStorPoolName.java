package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.InvalidNameException;

/**
 * Valid name of a linstor free space manager
 */
public class SharedStorPoolName extends GenericName
{
    private static final String RESERVED_CONNECTOR = ";";

    public static final int MIN_LENGTH = 1;
    public static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    // FIXME: as shown below a FreeSpaceMgrName might be a concatenation of
    // NodeName and StorPoolName. This means FreeSpaceName has to allow
    // at least as much as NodeName!
    public static final byte[] VALID_INNER_CHARS = {'_', '-', '.'};

    private final boolean shared;

    public SharedStorPoolName(String sharedStorPoolNameStr) throws InvalidNameException
    {
        super(sharedStorPoolNameStr);
        Checks.nameCheck(sharedStorPoolNameStr, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
        shared = !sharedStorPoolNameStr.contains(RESERVED_CONNECTOR);
    }

    public SharedStorPoolName(NodeName nodeName, StorPoolName storPoolName) throws InvalidNameException
    {
        super(nodeName.displayValue + RESERVED_CONNECTOR + storPoolName.displayValue);
        shared = false;
    }

    public static SharedStorPoolName restoreName(String sharedStorPoolNameStr) throws InvalidNameException
    {
        SharedStorPoolName ret;
        if (!sharedStorPoolNameStr.contains(RESERVED_CONNECTOR))
        {
            ret = new SharedStorPoolName(sharedStorPoolNameStr);
        }
        else
        {
            String[] parts = sharedStorPoolNameStr.split(RESERVED_CONNECTOR);
            if (parts.length != 2)
            {
                throw new InvalidNameException("Reserved name has incorrect number of parts", sharedStorPoolNameStr);
            }
            ret = new SharedStorPoolName(new NodeName(parts[0]), new StorPoolName(parts[1]));
        }
        return ret;
    }

    public static boolean isShared(String sharedStorPoolNameStr)
    {
        return sharedStorPoolNameStr != null && !sharedStorPoolNameStr.contains(RESERVED_CONNECTOR);
    }

    public boolean isShared()
    {
        return shared;
    }
}
