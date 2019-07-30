package com.linbit.linstor.core.identifier;

import com.linbit.InvalidNameException;
import com.linbit.Checks;
import com.linbit.GenericName;

/**
 * Valid name of a linstor key-value-store
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class KeyValueStoreName extends GenericName
{
    public static final int MIN_LENGTH = 2;
    public static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public KeyValueStoreName(String kvsName) throws InvalidNameException
    {
        super(kvsName);
        Checks.nameCheck(kvsName, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }
}
