package com.linbit.linstor.api.pojo;

import java.util.Collection;
import java.util.Map;

import com.linbit.linstor.VolumeNumber;

public class ResourceState
{
    private String rscName;
    private String nodeName;

    private boolean isPresent;
    private boolean requiresAdjust;
    private boolean isPrimary;

    private Map<VolumeNumber, ? extends VolumeState> volumeMap;

    public ResourceState()
    {
    }

    public ResourceState(
        String rscNameRef,
        String nodeNameRef,
        boolean isPresentRef,
        boolean requiresAdjustRef,
        boolean isPrimaryRef,
        Map<VolumeNumber, ? extends VolumeState> volumes
    )
    {
        rscName = rscNameRef;
        nodeName = nodeNameRef;
        isPresent = isPresentRef;
        requiresAdjust = requiresAdjustRef;
        isPrimary = isPrimaryRef;
        volumeMap = volumes;
    }

    public final void reset()
    {
        isPresent       = false;
        requiresAdjust  = false;
        isPrimary       = false;
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

    public Collection<? extends VolumeState> getVolumes()
    {
        return volumeMap.values();
    }

    public void setVolumes(Map<VolumeNumber, ? extends VolumeState> volumes)
    {
        this.volumeMap = volumes;
    }

    @Override
    public String toString()
    {
        StringBuilder rscActions = new StringBuilder();
        rscActions.append("Resource '").append(rscName).append("'\n");
        rscActions.append("    isPresent = ").append(this.isPresent()).append("\n");
        rscActions.append("    requiresAdjust = ").append(this.requiresAdjust()).append("\n");
        rscActions.append("    isPrimary = ").append(this.isPrimary()).append("\n");
        for (VolumeState vlmState : getVolumes())
        {
            rscActions.append(vlmState.toString());
        }
        return rscActions.toString();
    }
}
