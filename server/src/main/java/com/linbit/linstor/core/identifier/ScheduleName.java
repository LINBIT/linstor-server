package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.InvalidNameException;

public class ScheduleName extends GenericName
{
    public ScheduleName(String nodeName) throws InvalidNameException
    {
        super(nodeName);
        Checks.hostNameCheck(nodeName);
    }
}
