package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestDiskGroupCollection extends ExosRestBaseResponse
{
    @JsonProperty("disk-groups")
    public ExosDiskGroup[] diskGroups;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosDiskGroup
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("name")
        public String name;

        @JsonProperty("blocksize")
        public long blocksize;

        @JsonProperty("size")
        public String size;

        @JsonProperty("size-numeric")
        public long sizeNumeric;

        @JsonProperty("freespace")
        public String freespace;

        @JsonProperty("freespace-numeric")
        public long freespaceNumeric;

        @JsonProperty("raw-size")
        public String rawSize;

        @JsonProperty("raw-size-numeric")
        public long rawSizeNumeric;

        @JsonProperty("storage-type")
        public String storageType;

        @JsonProperty("storage-type-numeric")
        public long storageTypeNumeric;

        @JsonProperty("pool")
        public String pool;

        @JsonProperty("pool-serial-number")
        public String poolSerialNumber;

        @JsonProperty("storage-tier")
        public String storageTier;

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
        public String owner;

        @JsonProperty("owner-numeric")
        public long ownerNumeric;

        @JsonProperty("preferred-owner")
        public String preferredOwner;

        @JsonProperty("preferred-owner-numeric")
        public long preferredOwnerNumeric;

        @JsonProperty("raidtype")
        public String raidtype;

        @JsonProperty("raidtype-numeric")
        public long raidtypeNumeric;

        @JsonProperty("diskcount")
        public long diskcount;

        @JsonProperty("sparecount")
        public long sparecount;

        @JsonProperty("chunksize")
        public String chunksize;

        @JsonProperty("status")
        public String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("lun")
        public long lun;

        @JsonProperty("min-drive-size")
        public String minDriveSize;

        @JsonProperty("min-drive-size-numeric")
        public long minDriveSizeNumeric;

        @JsonProperty("create-date")
        public String createDate;

        @JsonProperty("create-date-numeric")
        public long createDateNumeric;

        @JsonProperty("cache-read-ahead")
        public String cacheReadAhead;

        @JsonProperty("cache-read-ahead-numeric")
        public long cacheReadAheadNumeric;

        @JsonProperty("cache-flush-period")
        public long cacheFlushPeriod;

        @JsonProperty("read-ahead-enabled")
        public String readAheadEnabled;

        @JsonProperty("read-ahead-enabled-numeric")
        public long readAheadEnabledNumeric;

        @JsonProperty("write-back-enabled")
        public String writeBackEnabled;

        @JsonProperty("write-back-enabled-numeric")
        public long writeBackEnabledNumeric;

        @JsonProperty("job-running")
        public String jobRunning;

        @JsonProperty("current-job")
        public String currentJob;

        @JsonProperty("current-job-numeric")
        public long currentJobNumeric;

        @JsonProperty("current-job-completion")
        public String currentJobCompletion;

        @JsonProperty("num-array-partitions")
        public long numArrayPartitions;

        @JsonProperty("largest-free-partition-space")
        public String largestFreePartitionSpace;

        @JsonProperty("largest-free-partition-space-numeric")
        public long largestFreePartitionSpaceNumeric;

        @JsonProperty("num-drives-per-low-level-array")
        public long numDrivesPerLowLevelArray;

        @JsonProperty("num-expansion-partitions")
        public long numExpansionPartitions;

        @JsonProperty("num-partition-segments")
        public long numPartitionSegments;

        @JsonProperty("new-partition-lba")
        public String newPartitionLba;

        @JsonProperty("new-partition-lba-numeric")
        public long newPartitionLbaNumeric;

        @JsonProperty("array-drive-type")
        public String arrayDriveType;

        @JsonProperty("array-drive-type-numeric")
        public long arrayDriveTypeNumeric;

        @JsonProperty("is-job-auto-abortable")
        public String isJobAutoAbortable;

        @JsonProperty("is-job-auto-abortable-numeric")
        public long isJobAutoAbortableNumeric;

        @JsonProperty("serial-number")
        public String serialNumber;

        @JsonProperty("blocks")
        public long blocks;

        @JsonProperty("disk-dsd-enable-vdisk")
        public String diskDsdEnableVdisk;

        @JsonProperty("disk-dsd-enable-vdisk-numeric")
        public long diskDsdEnableVdiskNumeric;

        @JsonProperty("disk-dsd-delay-vdisk")
        public long diskDsdDelayVdisk;

        @JsonProperty("scrub-duration-goal")
        public long scrubDurationGoal;

        @JsonProperty("adapt-target-spare-capacity")
        public String adaptTargetSpareCapacity;

        @JsonProperty("adapt-target-spare-capacity-numeric")
        public long adaptTargetSpareCapacityNumeric;

        @JsonProperty("adapt-actual-spare-capacity")
        public String adaptActualSpareCapacity;

        @JsonProperty("adapt-actual-spare-capacity-numeric")
        public long adaptActualSpareCapacityNumeric;

        @JsonProperty("adapt-critical-capacity")
        public String adaptCriticalCapacity;

        @JsonProperty("adapt-critical-capacity-numeric")
        public long adaptCriticalCapacityNumeric;

        @JsonProperty("adapt-degraded-capacity")
        public String adaptDegradedCapacity;

        @JsonProperty("adapt-degraded-capacity-numeric")
        public long adaptDegradedCapacityNumeric;

        @JsonProperty("adapt-linear-volume-boundary")
        public long adaptLinearVolumeBoundary;

        @JsonProperty("pool-sector-format")
        public String poolSectorFormat;

        @JsonProperty("pool-sector-format-numeric")
        public long poolSectorFormatNumeric;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("health-recommendation")
        public String healthRecommendation;
    }
}
