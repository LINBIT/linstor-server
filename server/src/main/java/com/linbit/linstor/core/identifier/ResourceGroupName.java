package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.InvalidNameException;

public class ResourceGroupName extends GenericName
{
    public static final int MIN_LENGTH = 2;
    public static final int MAX_LENGTH = 48;

    public static final byte[] VALID_CHARS = {'_'};
    public static final byte[] VALID_INNER_CHARS = {'_', '-'};

    public ResourceGroupName(String policyName) throws InvalidNameException
    {
        super(policyName);
        Checks.nameCheck(policyName, MIN_LENGTH, MAX_LENGTH, VALID_CHARS, VALID_INNER_CHARS);
    }
}
