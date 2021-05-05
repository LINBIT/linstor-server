package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;

public class RemoteName extends GenericName
{
    public RemoteName(String remoteNameRef) throws InvalidNameException
    {
        this(remoteNameRef, false);
    }

    public RemoteName(String remoteName, boolean internal) throws InvalidNameException
    {
        super(remoteName);
        if (!internal)
        {
            Checks.hostNameCheck(remoteName);
        }
        else if (!remoteName.startsWith("."))
        {
            throw new ImplementationError("Internal RemoteNames must begin with '.'!");
        }
    }
}
