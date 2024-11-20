package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.InvalidNameException;

/**
 * Valid name of a linstor snapshot
 */
public class SnapshotName extends GenericName
{
    private static final int MIN_LENGTH = 2;
    private static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_CHARS_INTERNAL =
    {
        '_', '.'
    };
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public SnapshotName(String snapshotName) throws InvalidNameException
    {
        this(snapshotName, false);
    }

    public SnapshotName(String snapshotName, boolean internal) throws InvalidNameException
    {
        super(snapshotName);
        Checks.nameCheck(
            snapshotName,
            MIN_LENGTH,
            MAX_LENGTH,
            internal ? VALID_CHARS_INTERNAL : VALID_CHARS,
            VALID_INNER_CHARS
        );
    }
}
