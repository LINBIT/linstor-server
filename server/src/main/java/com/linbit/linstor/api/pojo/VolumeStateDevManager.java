package com.linbit.linstor.api.pojo;

import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage.StorageDriver;

public class VolumeStateDevManager extends VolumeState
{

    /**
     * Indicates whether the resource should be deleted
     */
    protected boolean markedForDelete = false;

    /**
     * Whether to skip/ignore the volume in following steps
     */
    protected boolean skip = false;

    /**
     * Reference to the storage driver for the storage backend volume
     */
    protected StorageDriver driver = null;

    /**
     * Number of peer slots
     */
    private short peerSlots;

    /**
     * Indicates whether the disk needs resizing
     */
    private Boolean diskNeedsResize = false;

    public VolumeStateDevManager(VolumeNumber volNrRef)
    {
        vlmNr = volNrRef;
    }

    public boolean isMarkedForDelete()
    {
        return markedForDelete;
    }

    public void setMarkedForDelete(boolean markedForDeleteRef)
    {
        markedForDelete = markedForDeleteRef;
    }

    public boolean isSkip()
    {
        return skip;
    }

    public void setSkip(boolean skipRef)
    {
        skip = skipRef;
    }

    public StorageDriver getDriver()
    {
        return driver;
    }

    public void setDriver(StorageDriver driverRef)
    {
        driver = driverRef;
    }

    public short getPeerSlots()
    {
        return peerSlots;
    }

    public void setPeerSlots(short peerSlotsRef)
    {
        this.peerSlots = peerSlotsRef;
    }

    public Boolean diskNeedsResize()
    {
        return diskNeedsResize;
    }

    public void setDiskNeedsResize(Boolean diskNeedsResizeRef)
    {
        diskNeedsResize = diskNeedsResizeRef;
    }

    @Override
    public String toString()
    {
        StringBuilder vlmStateString = new StringBuilder();
        vlmStateString.append(super.toString());
        vlmStateString.append("        skip            = ").append(isSkip()).append("\n");
        vlmStateString.append("        driver          = ").append(getDriver()).append("\n");
        vlmStateString.append("        peerSlots       = ").append(getPeerSlots()).append("\n");
        return vlmStateString.toString();
    }
}
