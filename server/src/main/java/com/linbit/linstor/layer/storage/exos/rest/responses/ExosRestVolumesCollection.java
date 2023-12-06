package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestVolumesCollection extends ExosRestBaseResponse
{
    public ExosRestVolume[] volumes;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosRestVolume
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("virtual-disk-name")
        public String virtualDiskName;

        @JsonProperty("storage-pool-name")
        public String storagePoolName;

        @JsonProperty("volume-name")
        public String volumeName;

        @JsonProperty("size")
        public String size;

        @JsonProperty("size-numeric")
        public long sizeNumeric;

        @JsonProperty("total-size")
        public String totalSize;

        @JsonProperty("total-size-numeric")
        public long totalSizeNumeric;

        @JsonProperty("allocated-size")
        public String allocatedSize;

        @JsonProperty("allocated-size-numeric")
        public long allocatedSizeNumeric;

        @JsonProperty("storage-type")
        public String storageType;

        @JsonProperty("storage-type-numeric")
        public int storageTypeNumeric;

        @JsonProperty("preferred-owner")
        public String preferredOwner;

        @JsonProperty("preferred-owner-numeric")
        public long preferredOwnerNumeric;

        @JsonProperty("owner")
        public String owner;

        @JsonProperty("owner-numeric")
        public long ownerNumeric;

        @JsonProperty("serial-number")
        public String serialNumber;

        @JsonProperty("write-policy")
        public String writePolicy;

        @JsonProperty("write-policy-numeric")
        public int writePolicyNumeric;

        @JsonProperty("cache-optimization")
        public String cacheOptimization;

        @JsonProperty("cache-optimization-numeric")
        public int cacheOptimizationNumeric;

        @JsonProperty("read-ahead-size")
        public String readAheadSize;

        @JsonProperty("read-ahead-size-numeric")
        public int readAheadSizeNumeric;

        @JsonProperty("volume-type")
        public String volumeType;

        @JsonProperty("volume-type-numeric")
        public int volumeTypeNumeric;

        @JsonProperty("volume-class")
        public String volumeClass;

        @JsonProperty("volume-class-numeric")
        public int volumeClassNumeric;

        @JsonProperty("tier-affinity")
        public String tierAffinity;

        @JsonProperty("tier-affinity-numeric")
        public int tierAffinityNumeric;

        @JsonProperty("snapshot")
        public String snapshot;

        @JsonProperty("snapshot-retention-priority")
        public String snapshotRetentionPriority;

        @JsonProperty("snapshot-retention-priority-numeric")
        public int snapshotRetentionPriorityNumeric;

        @JsonProperty("volume-qualifier")
        public String volumeQualifier;

        @JsonProperty("volume-qualifier-numeric")
        public int volumeQualifierNumeric;

        @JsonProperty("blocksize")
        public long blocksize;

        @JsonProperty("blocks")
        public long blocks;

        @JsonProperty("capabilities")
        public String capabilities;

        @JsonProperty("volume-parent")
        public String volumeParent;

        @JsonProperty("snap-pool")
        public String snapPool;

        @JsonProperty("replication-set")
        public String replicationSet;

        @JsonProperty("attributes")
        public String attributes;

        @JsonProperty("virtual-disk-serial")
        public String virtualDiskSerial;

        @JsonProperty("volume-description")
        public String volumeDescription;

        @JsonProperty("wwn")
        public String wwn;

        @JsonProperty("progress")
        public String progress;

        @JsonProperty("progress-numeric")
        public int progressNumeric;

        @JsonProperty("container-name")
        public String containerName;

        @JsonProperty("container-serial")
        public String containerSerial;

        @JsonProperty("allowed-storage-tiers")
        public String allowedSstorageTiers;

        @JsonProperty("allowed-storage-tiers-numeric")
        public long allowedStorageTiersNumeric;

        @JsonProperty("threshold-percent-of-pool")
        public String thresholdPercentOfPool;

        @JsonProperty("reserved-size-in-pages")
        public long reservedSizeInPages;

        @JsonProperty("allocate-reserved-pages-first")
        public String allocateReservedPagesFirst;

        @JsonProperty("allocate-reserved-pages-first-numeric")
        public int allocateReservedPagesFirstNumeric;

        @JsonProperty("zero-init-page-on-allocation")
        public String zeroInitPageOnAllocation;

        @JsonProperty("zero-init-page-on-allocation-numeric")
        public int zeroInitPageOnAllocationNumeric;

        @JsonProperty("large-virtual-extents")
        public String largeVirtualExtents;

        @JsonProperty("large-virtual-extents-numeric")
        public int largeVirtualExtentsNumeric;

        @JsonProperty("raidtype")
        public String raidtype;

        @JsonProperty("raidtype-numeric")
        public int raidtypeNumeric;

        @JsonProperty("pi-format")
        public String piFormat;

        @JsonProperty("pi-format-numeric")
        public int piFormatNumeric;

        @JsonProperty("cs-replication-role")
        public String csReplicationRole;

        @JsonProperty("cs-copy-dest")
        public String csCopyDest;

        @JsonProperty("cs-copy-dest-numeric")
        public int csCopyDestNumeric;

        @JsonProperty("cs-copy-src")
        public String csCopySrc;

        @JsonProperty("cs-copy-src-numeric")
        public int csCopySrcNumeric;

        @JsonProperty("cs-primary")
        public String csPrimary;

        @JsonProperty("cs-primary-numeric")
        public int csPrimaryNumeric;

        @JsonProperty("cs-secondary")
        public String csSecondary;

        @JsonProperty("cs-secondary-numeric")
        public int csSecondaryNumeric;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public int healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("health-recommendation")
        public String healthRecommendation;

        @JsonProperty("volume-group")
        public String volumeGroup;

        @JsonProperty("group-key")
        public String groupKey;
    }
}
