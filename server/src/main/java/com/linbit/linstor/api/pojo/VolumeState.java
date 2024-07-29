package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.MinorNumber;

public class VolumeState
{
    protected @Nullable VolumeNumber vlmNr;
    protected @Nullable MinorNumber minorNr;

    /**
     * Indicates whether the volume is currently active (in use by DRBD)
     */
    protected Boolean isPresent = false;

    /**
     * Indicates whether a storage backend volume is present
     */
    protected Boolean hasDisk = false;

    /**
     * Indicates whether DRBD meta data is present on the storage backend volume
     *
     * To avoid overwriting existing meta data if the check for meta data fails,
     * the initial assumption is that there is existing meta data, and the check
     * attempts to prove that there is no meta data
     */
    protected Boolean hasMetaData = true;

    /**
     * Indicates whether the meta data present on the storage backend volume is
     * new. That is, no initial sync has occurred.
     */
    private Boolean metaDataIsNew = false;

    /**
     * Indicates whether a check for meta data should be performed
     */
    protected Boolean checkMetaData = true;

    /**
     * Indicates whether DRBD thinks the volume's backend storage volume has failed
     */
    protected Boolean diskFailed = false;

    /**
     * Net size (without DRBD meta data) of the volume in kiB
     */
    protected Long netSize = 0L;

    /**
     * Gross size (with internal DRBD meta data) of the volume in kiB
     */
    protected Long grossSize = 0L;

    /**
     * Drbd disk state
     */
    protected @Nullable String diskState = null;

    public VolumeState()
    {
    }

    public VolumeState(
        VolumeNumber vlmNrRef,
        MinorNumber minorNrRef,
        Boolean isPresentRef,
        Boolean hasDiskRef,
        Boolean hasMetaDataRef,
        Boolean checkMetaDataRef,
        Boolean diskFailedRef,
        Long netSizeRef,
        Long grossSizeRef,
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

    public final void reset()
    {
        isPresent       = false;
        hasDisk         = false;
        hasMetaData     = true;
        checkMetaData   = true;
        diskFailed      = false;
        netSize         = 0L;
        grossSize       = 0L;
        diskState       = null;
    }

    public VolumeNumber getVlmNr()
    {
        return vlmNr;
    }

    public MinorNumber getMinorNr()
    {
        return minorNr;
    }

    public Boolean isPresent()
    {
        return isPresent;
    }

    public Boolean hasDisk()
    {
        return hasDisk;
    }

    public Boolean hasMetaData()
    {
        return hasMetaData;
    }

    public Boolean metaDataIsNew()
    {
        return metaDataIsNew;
    }

    public Boolean isCheckMetaData()
    {
        return checkMetaData;
    }

    public Boolean isDiskFailed()
    {
        return diskFailed;
    }

    public Long getNetSize()
    {
        return netSize;
    }

    public Long getGrossSize()
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

    public void setPresent(Boolean present)
    {
        isPresent = present;
    }

    public void setHasDisk(Boolean hasDiskRef)
    {
        hasDisk = hasDiskRef;
    }

    public void setHasMetaData(Boolean hasMetaDataRef)
    {
        hasMetaData = hasMetaDataRef;
    }

    public void setMetaDataIsNew(Boolean metaDataIsNewRef)
    {
        metaDataIsNew = metaDataIsNewRef;
    }

    public void setCheckMetaData(Boolean checkMetaDataRef)
    {
        checkMetaData = checkMetaDataRef;
    }

    public void setDiskFailed(Boolean diskFailedRef)
    {
        diskFailed = diskFailedRef;
    }

    public void setNetSize(Long netSizeRef)
    {
        netSize = netSizeRef;
    }

    public void setGrossSize(Long grossSizeRef)
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
        vlmStateString.append("        metaDataIsNew = ").append(metaDataIsNew()).append("\n");
        vlmStateString.append("        checkMetaData = ").append(isCheckMetaData()).append("\n");
        vlmStateString.append("        netSize       = ").append(getNetSize()).append(" kiB\n");
        vlmStateString.append("        grossSize     = ").append(getGrossSize()).append(" kiB\n");
        vlmStateString.append("        diskState     = ").append(getDiskState()).append("\n");
        return vlmStateString.toString();
    }
}
