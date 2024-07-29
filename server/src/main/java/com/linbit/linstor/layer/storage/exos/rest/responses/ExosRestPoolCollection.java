package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestDiskGroupCollection.ExosDiskGroup;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestPoolCollection extends ExosRestBaseResponse
{
    public @Nullable ExosRestPool[] pools;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosRestPool
    {
        /*
         * do not use RespExosDiskGrouCollection as that also extends from ExosBaseResponse, while
         * this disk-group does not have a nested "status" JSON object
         */
        @JsonProperty("disk-groups")
        public @Nullable ExosDiskGroup[] diskGroups;

        @JsonProperty("tiers")
        public @Nullable RespExosTier[] tiers;

        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("name")
        public @Nullable String name;

        @JsonProperty("serial-number")
        public @Nullable String serialNumber;

        @JsonProperty("storage-type")
        public @Nullable String storageType;

        @JsonProperty("storage-type-numeric")
        public long storageTypeNumeric;

        @JsonProperty("blocksize")
        public long blocksize;

        @JsonProperty("total-size")
        public @Nullable String totalSize;

        @JsonProperty("total-size-numeric")
        public long totalSizeNumeric;

        @JsonProperty("total-avail")
        public @Nullable String totalAvail;

        @JsonProperty("total-avail-numeric")
        public long totalAvailNumeric;

        @JsonProperty("snap-size")
        public @Nullable String snapSize;

        @JsonProperty("snap-size-numeric")
        public long snapSizeNumeric;

        @JsonProperty("allocated-pages")
        public long allocatedPages;

        @JsonProperty("available-pages")
        public long availablePages;

        @JsonProperty("overcommit")
        public @Nullable String overcommit;

        @JsonProperty("overcommit-numeric")
        public long overcommitNumeric;

        @JsonProperty("over-committed")
        public @Nullable String overCommitted;

        @JsonProperty("over-committed-numeric")
        public long overCommittedNumeric;

        @JsonProperty("volumes")
        public long volumes;

        @JsonProperty("page-size")
        public @Nullable String pageSize;

        @JsonProperty("page-size-numeric")
        public long pageSizeNumeric;

        @JsonProperty("low-threshold")
        public @Nullable String lowThreshold;

        @JsonProperty("middle-threshold")
        public @Nullable String middleThreshold;

        @JsonProperty("high-threshold")
        public @Nullable String highThreshold;

        @JsonProperty("utility-running")
        public @Nullable String utilityRunning;

        @JsonProperty("utility-running-numeric")
        public long utilityRunningNumeric;

        @JsonProperty("preferred-owner")
        public @Nullable String preferredOwner;

        @JsonProperty("preferred-owner-numeric")
        public long preferredOwnerNumeric;

        @JsonProperty("owner")
        public @Nullable String owner;

        @JsonProperty("owner-numeric")
        public long ownerNumeric;

        @JsonProperty("rebalance")
        public @Nullable String rebalance;

        @JsonProperty("rebalance-numeric")
        public long rebalanceNumeric;

        @JsonProperty("migration")
        public @Nullable String migration;

        @JsonProperty("migration-numeric")
        public long migrationNumeric;

        @JsonProperty("zero-scan")
        public @Nullable String zeroScan;

        @JsonProperty("zero-scan-numeric")
        public long zeroScanNumeric;

        @JsonProperty("idle-page-check")
        public @Nullable String idlePageCheck;

        @JsonProperty("idle-page-check-numeric")
        public long idlePageCheckNumeric;

        @JsonProperty("read-flash-cache")
        public @Nullable String readFlashCache;

        @JsonProperty("read-flash-cache-numeric")
        public long readFlashCacheNumeric;

        @JsonProperty("metadata-vol-size")
        public @Nullable String metadataVolSize;

        @JsonProperty("metadata-vol-size-numeric")
        public long metadataVolSizeNumeric;

        @JsonProperty("total-rfc-size")
        public @Nullable String totalRfcSize;

        @JsonProperty("total-rfc-size-numeric")
        public long totalRfcSizeNumeric;

        @JsonProperty("available-rfc-size")
        public @Nullable String availableRfcSize;

        @JsonProperty("available-rfc-size-numeric")
        public long availableRfcSizeNumeric;

        @JsonProperty("reserved-size")
        public @Nullable String reservedSize;

        @JsonProperty("reserved-size-numeric")
        public long reservedSizeNumeric;

        @JsonProperty("reserved-unalloc-size")
        public @Nullable String reservedUnallocSize;

        @JsonProperty("reserved-unalloc-size-numeric")
        public long reservedUnallocSizeNumeric;

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

    public static class RespExosTier
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("serial-number")
        public @Nullable String serialNumber;

        @JsonProperty("pool")
        public @Nullable String pool;

        @JsonProperty("tier")
        public @Nullable String tier;

        @JsonProperty("tier-numeric")
        public long tierNumeric;

        @JsonProperty("pool-percentage")
        public long poolPercentage;

        @JsonProperty("diskcount")
        public long diskcount;

        @JsonProperty("raw-size")
        public @Nullable String rawSize;

        @JsonProperty("raw-size-numeric")
        public long rawSizeNumeric;

        @JsonProperty("total-size")
        public @Nullable String totalSize;

        @JsonProperty("total-size-numeric")
        public long totalSizeNumeric;

        @JsonProperty("allocated-size")
        public @Nullable String allocatedSize;

        @JsonProperty("allocated-size-numeric")
        public long allocatedSizeNumeric;

        @JsonProperty("available-size")
        public @Nullable String availableSize;

        @JsonProperty("available-size-numeric")
        public long availableSizeNumeric;

        @JsonProperty("affinity-size")
        public @Nullable String affinitySize;

        @JsonProperty("affinity-size-numeric")
        public long affinitySizeNumeric;
    }
}
