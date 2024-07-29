package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.Collection;
import java.util.Map;

public class ResourceState
{
    private @Nullable String rscName;
    private @Nullable String nodeName;

    private boolean isPresent;
    private boolean requiresAdjust;
    private boolean isPrimary;
    private boolean isSuspendedUser;

    private @Nullable Map<VolumeNumber, VolumeState> volumeMap;

    public ResourceState()
    {
    }

    public final void reset()
    {
        isPresent       = false;
        requiresAdjust  = false;
        isPrimary       = false;
        isSuspendedUser = false;
    }

    public String getRscName()
    {
        return rscName;
    }

    public void setRscName(String rscNameRef)
    {
        rscName = rscNameRef;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public void setNodeName(String nodeNameRef)
    {
        nodeName = nodeNameRef;
    }

    public boolean isPresent()
    {
        return isPresent;
    }

    public void setPresent(boolean present)
    {
        isPresent = present;
    }

    public boolean requiresAdjust()
    {
        return requiresAdjust;
    }

    public void setRequiresAdjust(boolean requiresAdjustRef)
    {
        requiresAdjust = requiresAdjustRef;
    }

    public boolean isPrimary()
    {
        return isPrimary;
    }

    public void setPrimary(boolean primary)
    {
        isPrimary = primary;
    }

    public boolean isSuspendedUser()
    {
        return isSuspendedUser;
    }

    public void setSuspendedUser(boolean suspendedUser)
    {
        isSuspendedUser = suspendedUser;
    }

    public Collection<VolumeState> getVolumes()
    {
        return volumeMap.values();
    }

    public void setVolumes(Map<VolumeNumber, VolumeState> volumes)
    {
        this.volumeMap = volumes;
    }

    public VolumeState getVolumeState(VolumeNumber volumeNumber)
    {
        return volumeMap.get(volumeNumber);
    }

    @Override
    public String toString()
    {
        StringBuilder rscActions = new StringBuilder();
        rscActions.append("Resource '").append(rscName).append("'\n");
        rscActions.append("    isPresent = ").append(this.isPresent()).append("\n");
        rscActions.append("    requiresAdjust = ").append(this.requiresAdjust()).append("\n");
        rscActions.append("    isPrimary = ").append(this.isPrimary()).append("\n");
        rscActions.append("    isSuspendedUser = ").append(this.isSuspendedUser()).append("\n");
        for (VolumeState vlmState : getVolumes())
        {
            rscActions.append(vlmState.toString());
        }
        return rscActions.toString();
    }

}
