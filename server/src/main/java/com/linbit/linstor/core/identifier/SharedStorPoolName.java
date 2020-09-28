package com.linbit.linstor.core.identifier;

import com.linbit.InvalidNameException;
import com.linbit.Checks;
import com.linbit.GenericName;

/**
 * Valid name of a linstor free space manager
 */
public class SharedStorPoolName extends GenericName
{
    private static final String RESERVED_CONNECTOR = ":";

    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_'};
    // FIXME: as shown below a FreeSpaceMgrName might be a concatenation of
    // NodeName and StorPoolName. This means FreeSpaceName has to allow
    // at least as much as NodeName!
    public static final byte[] VALID_INNER_CHARS = {'_', '-', '.'};

    public SharedStorPoolName(String sharedStorPoolNameStr) throws InvalidNameException
    {
        super(sharedStorPoolNameStr);
        Checks.nameCheck(sharedStorPoolNameStr, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }

    public SharedStorPoolName(NodeName nodeName, StorPoolName storPoolName) throws InvalidNameException
    {
        super(nodeName.displayValue + RESERVED_CONNECTOR + storPoolName.displayValue);
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
}
