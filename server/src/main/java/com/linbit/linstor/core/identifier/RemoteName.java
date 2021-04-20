package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.InvalidNameException;

public class RemoteName extends GenericName
{

    public RemoteName(String remoteName) throws InvalidNameException
    {
        super(remoteName);
        Checks.hostNameCheck(remoteName);
    }

}
