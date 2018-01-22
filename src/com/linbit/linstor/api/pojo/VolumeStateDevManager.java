package com.linbit.linstor.api.pojo;

import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage.StorageDriver;

public class VolumeStateDevManager extends VolumeState {

    /**
     * Indicates whether the resource should be deleted
     */
    protected boolean markedForDelete = false;

    /**
     * Whether to skip/ignore the volume in following steps
     */
    protected boolean skip = false;

    /**
     * Indicates whether a lookup for the volume's StorageDriver has already been performed
     * Note that this does not imply that the driver reference is non-null
     */
    protected boolean driverKnown = false;

    /**
     * Reference to the storage driver for the storage backend volume
     */
    protected StorageDriver driver    = null;

    /**
     * Name of the storage pool that is selected for the storage backend volume
     */
    protected StorPoolName storPoolName = null;

    /**
     * Name of the storage backend volume as known to the storage driver
     */
    protected String storVlmName      = null;

    public VolumeStateDevManager(VolumeNumber volNrRef, long netSizeSpec) {
        vlmNr = volNrRef;
        netSize = netSizeSpec;
    }

    public boolean isMarkedForDelete() {
        return markedForDelete;
    }

    public void setMarkedForDelete(boolean markedForDelete) {
        this.markedForDelete = markedForDelete;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    public boolean isDriverKnown() {
        return driverKnown;
    }

    public void setDriverKnown(boolean driverKnown) {
        this.driverKnown = driverKnown;
    }

    public StorageDriver getDriver() {
        return driver;
    }

    public void setDriver(StorageDriver driver) {
        this.driver = driver;
    }

    public StorPoolName getStorPoolName() {
        return storPoolName;
    }

    public void setStorPoolName(StorPoolName storPoolName) {
        this.storPoolName = storPoolName;
    }

    public String getStorVlmName() {
        return storVlmName;
    }

    public void setStorVlmName(String storVlmName) {
        this.storVlmName = storVlmName;
    }

    @Override
    public String toString() {
        StringBuilder vlmStateString = new StringBuilder();
        vlmStateString.append("    Volume ").append(getVlmNr().value).append("\n");
        vlmStateString.append("        skip          = ").append(isSkip()).append("\n");
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
