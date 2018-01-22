package com.linbit.linstor.api.pojo;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage.StorageDriver;

public class VolumeState {
    protected VolumeNumber vlmNr;
    protected MinorNumber minorNr;

    /**
     * Indicates whether the volume is currently active (in use by DRBD)
     */
    protected boolean isPresent = false;

    /**
     * Indicates whether a storage backend volume is present
     */
    protected boolean hasDisk = false;

    /**
     * Indicates whether DRBD meta data is present on the storage backend volume
     *
     * To avoid overwriting existing meta data if the check for meta data fails,
     * the initial assumption is that there is existing meta data, and the check
     * attempts to prove that there is no meta data
     */
    protected boolean hasMetaData = true;

    /**
     * Indicates whether a check for meta data should be performed
     */
    protected boolean checkMetaData = true;

    /**
     * Indicates whether DRBD thinks the volume's backend storage volume has failed
     */
    protected boolean diskFailed = false;

    /**
     * Net size (without DRBD meta data) of the volume in kiB
     */
    protected long netSize = 0L;

    /**
     * Gross size (with internal DRBD meta data) of the volume in kiB
     */
    protected long grossSize = 0L;

    public VolumeState() {
    }

    public VolumeState(
        VolumeNumber vlmNr,
        MinorNumber minorNr,
        boolean isPresent,
        boolean hasDisk,
        boolean hasMetaData,
        boolean checkMetaData,
        boolean diskFailed,
        long netSize,
        long grossSize
    ) {
        this.vlmNr = vlmNr;
        this.minorNr = minorNr;
        this.isPresent = isPresent;
        this.hasDisk = hasDisk;
        this.hasMetaData = hasMetaData;
        this.checkMetaData = checkMetaData;
        this.diskFailed = diskFailed;
        this.netSize = netSize;
        this.grossSize = grossSize;
    }

    public VolumeNumber getVlmNr() {
        return vlmNr;
    }

    public MinorNumber getMinorNr() {
        return minorNr;
    }

    public boolean isPresent() {
        return isPresent;
    }

    public boolean hasDisk() {
        return hasDisk;
    }

    public boolean hasMetaData() {
        return hasMetaData;
    }

    public boolean isCheckMetaData() {
        return checkMetaData;
    }

    public boolean isDiskFailed() {
        return diskFailed;
    }

    public long getNetSize() {
        return netSize;
    }

    public long getGrossSize() {
        return grossSize;
    }

    public void setVlmNr(VolumeNumber vlmNr) {
        this.vlmNr = vlmNr;
    }

    public void setMinorNr(MinorNumber minorNr) {
        this.minorNr = minorNr;
    }

    public void setPresent(boolean present) {
        isPresent = present;
    }

    public void setHasDisk(boolean hasDisk) {
        this.hasDisk = hasDisk;
    }

    public void setHasMetaData(boolean hasMetaData) {
        this.hasMetaData = hasMetaData;
    }

    public void setCheckMetaData(boolean checkMetaData) {
        this.checkMetaData = checkMetaData;
    }

    public void setDiskFailed(boolean diskFailed) {
        this.diskFailed = diskFailed;
    }

    public void setNetSize(long netSize) {
        this.netSize = netSize;
    }

    public void setGrossSize(long grossSize) {
        this.grossSize = grossSize;
    }

    @Override
    public String toString() {
        StringBuilder vlmStateString = new StringBuilder();
        vlmStateString.append("    Volume ").append(getVlmNr().value).append("\n");
        vlmStateString.append("        isPresent     = ").append(isPresent()).append("\n");
        vlmStateString.append("        hasDisk       = ").append(hasDisk()).append("\n");
        vlmStateString.append("        diskFailed    = ").append(isDiskFailed()).append("\n");
        vlmStateString.append("        hasMetaData   = ").append(hasMetaData()).append("\n");
        vlmStateString.append("        checkMetaData = ").append(isCheckMetaData()).append("\n");
        vlmStateString.append("        netSize       = ").append(getNetSize()).append(" kiB\n");
        vlmStateString.append("        grossSize     = ").append(getGrossSize()).append(" kiB\n");
        return vlmStateString.toString();
    }
}
