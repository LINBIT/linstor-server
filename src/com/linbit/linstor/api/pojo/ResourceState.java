package com.linbit.linstor.api.pojo;

import java.util.Collection;
import java.util.Map;

import com.linbit.linstor.VolumeNumber;

public class ResourceState {
    private String rscName;
    private String nodeName;

    private boolean isPresent;
    private boolean requiresAdjust;
    private boolean isPrimary;

    private Map<VolumeNumber, ? extends VolumeState> volumeMap;

    public ResourceState() {

    }

    public ResourceState(
        String rscName,
        String nodeName,
        boolean isPresent,
        boolean requiresAdjust,
        boolean isPrimary,
        Map<VolumeNumber, ? extends VolumeState> volumes
    ) {
        this.rscName = rscName;
        this.nodeName = nodeName;
        this.isPresent = isPresent;
        this.requiresAdjust = requiresAdjust;
        this.isPrimary = isPrimary;
        this.volumeMap = volumes;
    }

    public String getRscName() {
        return rscName;
    }

    public void setRscName(String rscName) {
        this.rscName = rscName;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public boolean isPresent() {
        return isPresent;
    }

    public void setPresent(boolean present) {
        isPresent = present;
    }

    public boolean requiresAdjust() {
        return requiresAdjust;
    }

    public void setRequiresAdjust(boolean requiresAdjust) {
        this.requiresAdjust = requiresAdjust;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public Collection<? extends VolumeState> getVolumes() {
        return volumeMap.values();
    }

    public void setVolumes(Map<VolumeNumber, ? extends VolumeState> volumes) {
        this.volumeMap = volumes;
    }

    @Override
    public String toString() {
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
