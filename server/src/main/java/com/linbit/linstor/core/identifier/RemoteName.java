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
    }

    /**
     * Ensures that the remoteName has a leading "." in order to avoid naming conflicts with user-remote names
     *
     * This method should only be used for StltRemotes, as those should also be thrown away after the backup is shipped
     * (successfully or not)
     *
     * @param remoteNameRef
     *
     * @return
     */
    public static RemoteName createInternal(String remoteNameRef)
    {
        String remoteNameToCreate;
        if (!remoteNameRef.startsWith("."))
        {
            remoteNameToCreate = "." + remoteNameRef;
        }
        else
        {
            remoteNameToCreate = remoteNameRef;
        }
        RemoteName remoteName;
        try
        {
            remoteName = new RemoteName(remoteNameToCreate, true);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Internal remote names should not throw InvalidNameExceptions", exc);
        }
        return remoteName;
    }
}
