package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestVolumesCollection extends ExosRestBaseResponse
{
    public @Nullable ExosRestVolume[] volumes;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosRestVolume
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("virtual-disk-name")
        public @Nullable String virtualDiskName;

        @JsonProperty("storage-pool-name")
        public @Nullable String storagePoolName;

        @JsonProperty("volume-name")
        public @Nullable String volumeName;

        @JsonProperty("size")
        public @Nullable String size;

        @JsonProperty("size-numeric")
        public long sizeNumeric;

        @JsonProperty("total-size")
        public @Nullable String totalSize;

        @JsonProperty("total-size-numeric")
        public long totalSizeNumeric;

        @JsonProperty("allocated-size")
        public @Nullable String allocatedSize;

        @JsonProperty("allocated-size-numeric")
        public long allocatedSizeNumeric;

        @JsonProperty("storage-type")
        public @Nullable String storageType;

        @JsonProperty("storage-type-numeric")
        public int storageTypeNumeric;

        @JsonProperty("preferred-owner")
        public @Nullable String preferredOwner;

        @JsonProperty("preferred-owner-numeric")
        public long preferredOwnerNumeric;

        @JsonProperty("owner")
        public @Nullable String owner;

        @JsonProperty("owner-numeric")
        public long ownerNumeric;

        @JsonProperty("serial-number")
        public @Nullable String serialNumber;

        @JsonProperty("write-policy")
        public @Nullable String writePolicy;

        @JsonProperty("write-policy-numeric")
        public int writePolicyNumeric;

        @JsonProperty("cache-optimization")
        public @Nullable String cacheOptimization;

        @JsonProperty("cache-optimization-numeric")
        public int cacheOptimizationNumeric;

        @JsonProperty("read-ahead-size")
        public @Nullable String readAheadSize;

        @JsonProperty("read-ahead-size-numeric")
        public int readAheadSizeNumeric;

        @JsonProperty("volume-type")
        public @Nullable String volumeType;

        @JsonProperty("volume-type-numeric")
        public int volumeTypeNumeric;

        @JsonProperty("volume-class")
        public @Nullable String volumeClass;

        @JsonProperty("volume-class-numeric")
        public int volumeClassNumeric;

        @JsonProperty("tier-affinity")
        public @Nullable String tierAffinity;

        @JsonProperty("tier-affinity-numeric")
        public int tierAffinityNumeric;

        @JsonProperty("snapshot")
        public @Nullable String snapshot;

        @JsonProperty("snapshot-retention-priority")
        public @Nullable String snapshotRetentionPriority;

        @JsonProperty("snapshot-retention-priority-numeric")
        public int snapshotRetentionPriorityNumeric;

        @JsonProperty("volume-qualifier")
        public @Nullable String volumeQualifier;

        @JsonProperty("volume-qualifier-numeric")
        public int volumeQualifierNumeric;

        @JsonProperty("blocksize")
        public long blocksize;

        @JsonProperty("blocks")
        public long blocks;

        @JsonProperty("capabilities")
        public @Nullable String capabilities;

        @JsonProperty("volume-parent")
        public @Nullable String volumeParent;

        @JsonProperty("snap-pool")
        public @Nullable String snapPool;

        @JsonProperty("replication-set")
        public @Nullable String replicationSet;

        @JsonProperty("attributes")
        public @Nullable String attributes;

        @JsonProperty("virtual-disk-serial")
        public @Nullable String virtualDiskSerial;

        @JsonProperty("volume-description")
        public @Nullable String volumeDescription;

        @JsonProperty("wwn")
        public @Nullable String wwn;

        @JsonProperty("progress")
        public @Nullable String progress;

        @JsonProperty("progress-numeric")
        public int progressNumeric;

        @JsonProperty("container-name")
        public @Nullable String containerName;

        @JsonProperty("container-serial")
        public @Nullable String containerSerial;

        @JsonProperty("allowed-storage-tiers")
        public @Nullable String allowedSstorageTiers;

        @JsonProperty("allowed-storage-tiers-numeric")
        public long allowedStorageTiersNumeric;

        @JsonProperty("threshold-percent-of-pool")
        public @Nullable String thresholdPercentOfPool;

        @JsonProperty("reserved-size-in-pages")
        public long reservedSizeInPages;

        @JsonProperty("allocate-reserved-pages-first")
        public @Nullable String allocateReservedPagesFirst;

        @JsonProperty("allocate-reserved-pages-first-numeric")
        public int allocateReservedPagesFirstNumeric;

        @JsonProperty("zero-init-page-on-allocation")
        public @Nullable String zeroInitPageOnAllocation;

        @JsonProperty("zero-init-page-on-allocation-numeric")
        public int zeroInitPageOnAllocationNumeric;

        @JsonProperty("large-virtual-extents")
        public @Nullable String largeVirtualExtents;

        @JsonProperty("large-virtual-extents-numeric")
        public int largeVirtualExtentsNumeric;

        @JsonProperty("raidtype")
        public @Nullable String raidtype;

        @JsonProperty("raidtype-numeric")
        public int raidtypeNumeric;

        @JsonProperty("pi-format")
        public @Nullable String piFormat;

        @JsonProperty("pi-format-numeric")
        public int piFormatNumeric;

        @JsonProperty("cs-replication-role")
        public @Nullable String csReplicationRole;

        @JsonProperty("cs-copy-dest")
        public @Nullable String csCopyDest;

        @JsonProperty("cs-copy-dest-numeric")
        public int csCopyDestNumeric;

        @JsonProperty("cs-copy-src")
        public @Nullable String csCopySrc;

        @JsonProperty("cs-copy-src-numeric")
        public int csCopySrcNumeric;

        @JsonProperty("cs-primary")
        public @Nullable String csPrimary;

        @JsonProperty("cs-primary-numeric")
        public int csPrimaryNumeric;

        @JsonProperty("cs-secondary")
        public @Nullable String csSecondary;

        @JsonProperty("cs-secondary-numeric")
        public int csSecondaryNumeric;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public int healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("health-recommendation")
        public @Nullable String healthRecommendation;

        @JsonProperty("volume-group")
        public @Nullable String volumeGroup;

        @JsonProperty("group-key")
        public @Nullable String groupKey;
    }
}
