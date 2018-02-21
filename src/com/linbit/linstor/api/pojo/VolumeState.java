package com.linbit.linstor.api.pojo;

import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.VolumeNumber;

public class VolumeState
{
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

    /**
     * Drbd disk state
     */
    protected String diskState = null;

    public VolumeState()
    {
    }

    public VolumeState(
        VolumeNumber vlmNrRef,
        MinorNumber minorNrRef,
        boolean isPresentRef,
        boolean hasDiskRef,
        boolean hasMetaDataRef,
        boolean checkMetaDataRef,
        boolean diskFailedRef,
        long netSizeRef,
        long grossSizeRef,
        final String diskStateRef
    )
    {
        vlmNr = vlmNrRef;
        minorNr = minorNrRef;
        isPresent = isPresentRef;
        hasDisk = hasDiskRef;
        hasMetaData = hasMetaDataRef;
        checkMetaData = checkMetaDataRef;
        diskFailed = diskFailedRef;
        netSize = netSizeRef;
        grossSize = grossSizeRef;
        diskState = diskStateRef;
    }

    public VolumeNumber getVlmNr()
    {
        return vlmNr;
    }

    public MinorNumber getMinorNr()
    {
        return minorNr;
    }

    public boolean isPresent()
    {
        return isPresent;
    }

    public boolean hasDisk()
    {
        return hasDisk;
    }

    public boolean hasMetaData()
    {
        return hasMetaData;
    }

    public boolean isCheckMetaData()
    {
        return checkMetaData;
    }

    public boolean isDiskFailed()
    {
        return diskFailed;
    }

    public long getNetSize()
    {
        return netSize;
    }

    public long getGrossSize()
    {
        return grossSize;
    }

    public String getDiskState()
    {
        return diskState;
    }

    public void setVlmNr(VolumeNumber vlmNrRef)
    {
        vlmNr = vlmNrRef;
    }

    public void setMinorNr(MinorNumber minorNrRef)
    {
        this.minorNr = minorNrRef;
    }

    public void setPresent(boolean present)
    {
        isPresent = present;
    }

    public void setHasDisk(boolean hasDiskRef)
    {
        hasDisk = hasDiskRef;
    }

    public void setHasMetaData(boolean hasMetaDataRef)
    {
        hasMetaData = hasMetaDataRef;
    }

    public void setCheckMetaData(boolean checkMetaDataRef)
    {
        checkMetaData = checkMetaDataRef;
    }

    public void setDiskFailed(boolean diskFailedRef)
    {
        diskFailed = diskFailedRef;
    }

    public void setNetSize(long netSizeRef)
    {
        netSize = netSizeRef;
    }

    public void setGrossSize(long grossSizeRef)
    {
        grossSize = grossSizeRef;
    }

    public void setDiskState(final String diskStateRef)
    {
        diskState = diskStateRef;
    }

    @Override
    public String toString()
    {
        StringBuilder vlmStateString = new StringBuilder();
        vlmStateString.append("    Volume ").append(getVlmNr().value).append("\n");
        vlmStateString.append("        isPresent     = ").append(isPresent()).append("\n");
        vlmStateString.append("        hasDisk       = ").append(hasDisk()).append("\n");
        vlmStateString.append("        diskFailed    = ").append(isDiskFailed()).append("\n");
        vlmStateString.append("        hasMetaData   = ").append(hasMetaData()).append("\n");
        vlmStateString.append("        checkMetaData = ").append(isCheckMetaData()).append("\n");
        vlmStateString.append("        netSize       = ").append(getNetSize()).append(" kiB\n");
        vlmStateString.append("        grossSize     = ").append(getGrossSize()).append(" kiB\n");
        vlmStateString.append("        diskState     = ").append(getDiskState()).append("\n");
        return vlmStateString.toString();
    }
}
