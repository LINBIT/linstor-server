package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

import java.util.Map;

public interface BackupApi
{
    /**
     * Unique ID of this backup within this cluster.
     * Something like concatenating resourceName and snapKey should be enough
     */
    String getId();

    /**
     * Snapshot name, usually in the format of "back_{startTimestamp}"
     */
    String getSnapKey();

    /**
     * Resource name
     */
    String getResourceName();

    /**
     * time when the backup was uploaded started, in the format "yyyyMMdd_HHmmss"
     */
    String getStartTime();

    /**
     * Machine readable version of {@link #getStartTime()}
     */
    Long getStartTimestamp();

    /**
     * time when the backup was uploaded completely, in the format "yyyyMMdd_HHmmss"
     * is null if the backup is not in the meta-file or the meta-file does not exist
     * this can happen if the shipping failed or a full backup has yet to finish uploading all its volume-snapshots
     * (or someone deleted something...)
     */
    String getFinishedTime();

    /**
     * Machine readable version of {@link #getFinishedTime()}
     */
    Long getFinishedTimestamp();

    /**
     * name of the node that was used to upload this snapshot
     * is null if the backup is not in the meta-file or the meta-file does not exist
     * this can happen if the shipping failed or a full backup has yet to finish uploading all its volume-snapshots
     * (or someone deleted something...)
     */
    String getOriginNodeName();

    /**
     * is true if some volumes of the backup are still being uploaded from the current linstor cluster, and false
     * otherwise
     * can be null if the snapshot definition can not be found on this linstor cluster
     */
    Boolean isShipping();

    /**
     * is true if the backup was uploaded successfully from the current linstor cluster, and false otherwise
     * is null if shipping is true or if the snapshot definition can not be found on this linstor cluster
     */
    Boolean successful();

    /**
     * is true in case of S3 if the meta-file exists and all entries in its backup-list are available in the same bucket
     */
    Boolean isRestoreable();

    /**
     * maps vlmNr to full backup name as saved in s3 ({rscName}_{vlmNr}_back_{timestampFullBackup}_{timestampIncBackup})
     * should always contain at least 1 entry
     */
    Map<Integer, ? extends BackupVlmApi> getVlms();

    /**
     * null if the current backup is a full backup.
     * otherwise this will reference the base backup, which might be a full or an incremental backup
     */
    @Nullable
    String getBasedOnId();

    BackupS3Api getS3();

    public interface BackupVlmApi
    {
        /**
         * The volume number
         */
        long getVlmNr();

        /**
         * time when this volume of the backup backup was uploaded completely, in the format "yyyyMMdd_HHmmss"
         * is null if the backup is not in the meta-file or the meta-file does not exist
         * this can happen if the shipping failed or a full backup has yet to finish uploading all its volume-snapshots
         * (or someone deleted something...)
         */
        String getFinishedTime();

        /**
         * Machine readable version of {@link #getFinishedTime()}
         */
        Long getFinishedTimestamp();

        /**
         * s3 specific properties for this volume
         * might be null
         */
        BackupVlmS3Api getS3();
    }

    public interface BackupVlmS3Api
    {
        /**
         * Name of the s3 in the format "{rscName}{rscNameSuffix}_{vlmNr}_back_{startTimestamp}"
         */
        String getS3Key();
    }

    public interface BackupS3Api
    {
        /**
         * Name of the meta file in s3 in the format "{rscName}_back_{startTimestamp}.meta"
         * The meta-file might not exist
         */
        String getMetaName();
    }

}
