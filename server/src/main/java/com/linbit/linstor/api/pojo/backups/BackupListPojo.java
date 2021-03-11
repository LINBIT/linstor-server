package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.core.apis.BackupListApi;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class BackupListPojo implements BackupListApi
{
    /**
     * name of the snapshot, in the format "back_{timestamp}"
     */
    private final String snapKey;
    /**
     * name of the meta-file, in the format "{rscName}_back_{timestamp}.meta"
     * the meta-file might not exist
     */
    private final String metaName;
    /**
     * time when the snapshot was uploaded completely, in the format "yyyyMMdd_HHmmss"
     * is null if the backup is not in the meta-file or the meta-file does not exist
     * this can happen if the shipping failed or a full backup has yet to finish uploading all its volume-snapshots
     * (or someone deleted something...)
     */
    private final String finishedTime;
    /**
     * machine-readable version of finishedTime
     */
    private final Long finishedTimestamp;
    /**
     * name of the node that was used to upload this snapshot
     * is null if the backup is not in the meta-file or the meta-file does not exist
     * this can happen if the shipping failed or a full backup has yet to finish uploading all its volume-snapshots
     * (or someone deleted something...)
     */
    private final String node;
    /**
     * is true if some volumes of the backup are still being uploaded from the current linstor cluster, and false
     * otherwise
     * can be null if the snapshot definition can not be found on this linstor cluster
     */
    private final Boolean shipping;
    /**
     * is true if the backup was uploaded successfully from the current linstor cluster, and false otherwise
     * is null if shipping is true or if the snapshot definition can not be found on this linstor cluster
     */
    private final Boolean success;
    /**
     * is true if the meta-file exists and all entries in its backup-list are available in the same bucket
     */
    private final Boolean restoreable;
    /**
     * maps vlmNr to full backup name as saved in s3 ({rscName}_{vlmNr}_back_{timestampFullBackup}_{timestampIncBackup})
     * should always contain at least 1 entry
     */
    private final Map<String, String> vlms;
    /**
     * only for full backups, is null for all elements in the list
     */
    private final List<BackupListApi> inc;

    public BackupListPojo(
        String snapKeyRef,
        String metaNameRef,
        @Nullable String finishedTimeRef,
        @Nullable Long finishedTimestampRef,
        @Nullable String nodeRef,
        @Nullable Boolean shippingRef,
        @Nullable Boolean successRef,
        @Nullable Boolean restoreableRef,
        Map<String, String> vlmsRef,
        @Nullable List<BackupListApi> incRef
    )
    {
        snapKey = snapKeyRef;
        metaName = metaNameRef;
        finishedTime = finishedTimeRef;
        finishedTimestamp = finishedTimestampRef;
        node = nodeRef;
        shipping = shippingRef;
        success = successRef;
        this.restoreable = restoreableRef;
        vlms = vlmsRef;
        inc = incRef;
    }

    @Override
    public String getSnapKey()
    {
        return snapKey;
    }

    @Override
    public String getMetaName()
    {
        return metaName;
    }

    @Override
    public String getFinishedTime()
    {
        return finishedTime;
    }

    @Override
    public Long getFinishedTimestamp()
    {
        return finishedTimestamp;
    }

    @Override
    public String getNode()
    {
        return node;
    }

    @Override
    public Boolean isShipping()
    {
        return shipping;
    }

    @Override
    public Boolean successful()
    {
        return success;
    }

    @Override
    public Boolean isRestoreable()
    {
        return restoreable;
    }

    @Override
    public Map<String, String> getVlms()
    {
        return vlms;
    }

    @Override
    public List<BackupListApi> getInc()
    {
        return inc;
    }

}
