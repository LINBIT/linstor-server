package com.linbit.linstor.core.identifier;

import com.linbit.Checks;
import com.linbit.GenericName;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;

import java.util.UUID;

public class RemoteName extends GenericName
{
    private static final String FORMAT_STLT_REMOTE = ".stlt;%s_%s_%s"; // ".stlt;$rscName_$snapName_$UUID"
    private static final String FORMAT_EBS_REMOTE = ".ebs;%s"; // ".ebs;$nodeName"

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

    public static RemoteName createStltRemoteName(String rscNameRef, String snapshotNameRef, UUID uuidRef)
    {
        return createInternal(String.format(FORMAT_STLT_REMOTE, rscNameRef, snapshotNameRef, uuidRef.toString()));
    }

    public static RemoteName createEbsRemoteName(String ebsRemoteNameRef)
    {
        return createInternal(String.format(FORMAT_EBS_REMOTE, ebsRemoteNameRef));
    }
}
