package com.linbit.linstor.storage;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.InvalidNameException;

/**
 * Name of a storage driver
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class StorageDriverName extends GenericName
{
    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 32;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public StorageDriverName(String driverName) throws InvalidNameException
    {
        super(driverName);
        Checks.nameCheck(driverName, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }
}
