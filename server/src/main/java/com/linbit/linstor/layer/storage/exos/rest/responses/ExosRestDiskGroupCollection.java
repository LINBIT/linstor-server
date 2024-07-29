package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestDiskGroupCollection extends ExosRestBaseResponse
{
    @JsonProperty("disk-groups")
    public @Nullable ExosDiskGroup[] diskGroups;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosDiskGroup
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("name")
        public @Nullable String name;

        @JsonProperty("blocksize")
        public long blocksize;

        @JsonProperty("size")
        public @Nullable String size;

        @JsonProperty("size-numeric")
        public long sizeNumeric;

        @JsonProperty("freespace")
        public @Nullable String freespace;

        @JsonProperty("freespace-numeric")
        public long freespaceNumeric;

        @JsonProperty("raw-size")
        public @Nullable String rawSize;

        @JsonProperty("raw-size-numeric")
        public long rawSizeNumeric;

        @JsonProperty("storage-type")
        public @Nullable String storageType;

        @JsonProperty("storage-type-numeric")
        public long storageTypeNumeric;

        @JsonProperty("pool")
        public @Nullable String pool;

        @JsonProperty("pool-serial-number")
        public @Nullable String poolSerialNumber;

        @JsonProperty("storage-tier")
        public @Nullable String storageTier;

        @JsonProperty("storage-tier-numeric")
        public long storageTierNumeric;

        @JsonProperty("total-pages")
        public long totalPages;

        @JsonProperty("allocated-pages")
        public long allocatedPages;

        @JsonProperty("available-pages")
        public long availablePages;

        @JsonProperty("pool-percentage")
        public long poolPercentage;

        @JsonProperty("performance-rank")
        public long performanceRank;

        @JsonProperty("owner")
        public @Nullable String owner;

        @JsonProperty("owner-numeric")
        public long ownerNumeric;

        @JsonProperty("preferred-owner")
        public @Nullable String preferredOwner;

        @JsonProperty("preferred-owner-numeric")
        public long preferredOwnerNumeric;

        @JsonProperty("raidtype")
        public @Nullable String raidtype;

        @JsonProperty("raidtype-numeric")
        public long raidtypeNumeric;

        @JsonProperty("diskcount")
        public long diskcount;

        @JsonProperty("sparecount")
        public long sparecount;

        @JsonProperty("chunksize")
        public @Nullable String chunksize;

        @JsonProperty("status")
        public @Nullable String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("lun")
        public long lun;

        @JsonProperty("min-drive-size")
        public @Nullable String minDriveSize;

        @JsonProperty("min-drive-size-numeric")
        public long minDriveSizeNumeric;

        @JsonProperty("create-date")
        public @Nullable String createDate;

        @JsonProperty("create-date-numeric")
        public long createDateNumeric;

        @JsonProperty("cache-read-ahead")
        public @Nullable String cacheReadAhead;

        @JsonProperty("cache-read-ahead-numeric")
        public long cacheReadAheadNumeric;

        @JsonProperty("cache-flush-period")
        public long cacheFlushPeriod;

        @JsonProperty("read-ahead-enabled")
        public @Nullable String readAheadEnabled;

        @JsonProperty("read-ahead-enabled-numeric")
        public long readAheadEnabledNumeric;

        @JsonProperty("write-back-enabled")
        public @Nullable String writeBackEnabled;

        @JsonProperty("write-back-enabled-numeric")
        public long writeBackEnabledNumeric;

        @JsonProperty("job-running")
        public @Nullable String jobRunning;

        @JsonProperty("current-job")
        public @Nullable String currentJob;

        @JsonProperty("current-job-numeric")
        public long currentJobNumeric;

        @JsonProperty("current-job-completion")
        public @Nullable String currentJobCompletion;

        @JsonProperty("num-array-partitions")
        public long numArrayPartitions;

        @JsonProperty("largest-free-partition-space")
        public @Nullable String largestFreePartitionSpace;

        @JsonProperty("largest-free-partition-space-numeric")
        public long largestFreePartitionSpaceNumeric;

        @JsonProperty("num-drives-per-low-level-array")
        public long numDrivesPerLowLevelArray;

        @JsonProperty("num-expansion-partitions")
        public long numExpansionPartitions;

        @JsonProperty("num-partition-segments")
        public long numPartitionSegments;

        @JsonProperty("new-partition-lba")
        public @Nullable String newPartitionLba;

        @JsonProperty("new-partition-lba-numeric")
        public long newPartitionLbaNumeric;

        @JsonProperty("array-drive-type")
        public @Nullable String arrayDriveType;

        @JsonProperty("array-drive-type-numeric")
        public long arrayDriveTypeNumeric;

        @JsonProperty("is-job-auto-abortable")
        public @Nullable String isJobAutoAbortable;

        @JsonProperty("is-job-auto-abortable-numeric")
        public long isJobAutoAbortableNumeric;

        @JsonProperty("serial-number")
        public @Nullable String serialNumber;

        @JsonProperty("blocks")
        public long blocks;

        @JsonProperty("disk-dsd-enable-vdisk")
        public @Nullable String diskDsdEnableVdisk;

        @JsonProperty("disk-dsd-enable-vdisk-numeric")
        public long diskDsdEnableVdiskNumeric;

        @JsonProperty("disk-dsd-delay-vdisk")
        public long diskDsdDelayVdisk;

        @JsonProperty("scrub-duration-goal")
        public long scrubDurationGoal;

        @JsonProperty("adapt-target-spare-capacity")
        public @Nullable String adaptTargetSpareCapacity;

        @JsonProperty("adapt-target-spare-capacity-numeric")
        public long adaptTargetSpareCapacityNumeric;

        @JsonProperty("adapt-actual-spare-capacity")
        public @Nullable String adaptActualSpareCapacity;

        @JsonProperty("adapt-actual-spare-capacity-numeric")
        public long adaptActualSpareCapacityNumeric;

        @JsonProperty("adapt-critical-capacity")
        public @Nullable String adaptCriticalCapacity;

        @JsonProperty("adapt-critical-capacity-numeric")
        public long adaptCriticalCapacityNumeric;

        @JsonProperty("adapt-degraded-capacity")
        public @Nullable String adaptDegradedCapacity;

        @JsonProperty("adapt-degraded-capacity-numeric")
        public long adaptDegradedCapacityNumeric;

        @JsonProperty("adapt-linear-volume-boundary")
        public long adaptLinearVolumeBoundary;

        @JsonProperty("pool-sector-format")
        public @Nullable String poolSectorFormat;

        @JsonProperty("pool-sector-format-numeric")
        public long poolSectorFormatNumeric;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("health-recommendation")
        public @Nullable String healthRecommendation;
    }
}
