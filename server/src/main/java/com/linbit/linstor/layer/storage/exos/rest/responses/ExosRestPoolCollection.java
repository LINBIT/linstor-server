package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestDiskGroupCollection.ExosDiskGroup;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestPoolCollection extends ExosRestBaseResponse
{
    public ExosRestPool[] pools;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosRestPool
    {
        /*
         * do not use RespExosDiskGrouCollection as that also extends from ExosBaseResponse, while
         * this disk-group does not have a nested "status" JSON object
         */
        @JsonProperty("disk-groups")
        public ExosDiskGroup[] diskGroups;

        @JsonProperty("tiers")
        public RespExosTier[] tiers;

        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("name")
        public String name;

        @JsonProperty("serial-number")
        public String serialNumber;

        @JsonProperty("storage-type")
        public String storageType;

        @JsonProperty("storage-type-numeric")
        public long storageTypeNumeric;

        @JsonProperty("blocksize")
        public long blocksize;

        @JsonProperty("total-size")
        public String totalSize;

        @JsonProperty("total-size-numeric")
        public long totalSizeNumeric;

        @JsonProperty("total-avail")
        public String totalAvail;

        @JsonProperty("total-avail-numeric")
        public long totalAvailNumeric;

        @JsonProperty("snap-size")
        public String snapSize;

        @JsonProperty("snap-size-numeric")
        public long snapSizeNumeric;

        @JsonProperty("allocated-pages")
        public long allocatedPages;

        @JsonProperty("available-pages")
        public long availablePages;

        @JsonProperty("overcommit")
        public String overcommit;

        @JsonProperty("overcommit-numeric")
        public long overcommitNumeric;

        @JsonProperty("over-committed")
        public String overCommitted;

        @JsonProperty("over-committed-numeric")
        public long overCommittedNumeric;

        @JsonProperty("volumes")
        public long volumes;

        @JsonProperty("page-size")
        public String pageSize;

        @JsonProperty("page-size-numeric")
        public long pageSizeNumeric;

        @JsonProperty("low-threshold")
        public String lowThreshold;

        @JsonProperty("middle-threshold")
        public String middleThreshold;

        @JsonProperty("high-threshold")
        public String highThreshold;

        @JsonProperty("utility-running")
        public String utilityRunning;

        @JsonProperty("utility-running-numeric")
        public long utilityRunningNumeric;

        @JsonProperty("preferred-owner")
        public String preferredOwner;

        @JsonProperty("preferred-owner-numeric")
        public long preferredOwnerNumeric;

        @JsonProperty("owner")
        public String owner;

        @JsonProperty("owner-numeric")
        public long ownerNumeric;

        @JsonProperty("rebalance")
        public String rebalance;

        @JsonProperty("rebalance-numeric")
        public long rebalanceNumeric;

        @JsonProperty("migration")
        public String migration;

        @JsonProperty("migration-numeric")
        public long migrationNumeric;

        @JsonProperty("zero-scan")
        public String zeroScan;

        @JsonProperty("zero-scan-numeric")
        public long zeroScanNumeric;

        @JsonProperty("idle-page-check")
        public String idlePageCheck;

        @JsonProperty("idle-page-check-numeric")
        public long idlePageCheckNumeric;

        @JsonProperty("read-flash-cache")
        public String readFlashCache;

        @JsonProperty("read-flash-cache-numeric")
        public long readFlashCacheNumeric;

        @JsonProperty("metadata-vol-size")
        public String metadataVolSize;

        @JsonProperty("metadata-vol-size-numeric")
        public long metadataVolSizeNumeric;

        @JsonProperty("total-rfc-size")
        public String totalRfcSize;

        @JsonProperty("total-rfc-size-numeric")
        public long totalRfcSizeNumeric;

        @JsonProperty("available-rfc-size")
        public String availableRfcSize;

        @JsonProperty("available-rfc-size-numeric")
        public long availableRfcSizeNumeric;

        @JsonProperty("reserved-size")
        public String reservedSize;

        @JsonProperty("reserved-size-numeric")
        public long reservedSizeNumeric;

        @JsonProperty("reserved-unalloc-size")
        public String reservedUnallocSize;

        @JsonProperty("reserved-unalloc-size-numeric")
        public long reservedUnallocSizeNumeric;

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

    public static class RespExosTier
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("serial-number")
        public String serialNumber;

        @JsonProperty("pool")
        public String pool;

        @JsonProperty("tier")
        public String tier;

        @JsonProperty("tier-numeric")
        public long tierNumeric;

        @JsonProperty("pool-percentage")
        public long poolPercentage;

        @JsonProperty("diskcount")
        public long diskcount;

        @JsonProperty("raw-size")
        public String rawSize;

        @JsonProperty("raw-size-numeric")
        public long rawSizeNumeric;

        @JsonProperty("total-size")
        public String totalSize;

        @JsonProperty("total-size-numeric")
        public long totalSizeNumeric;

        @JsonProperty("allocated-size")
        public String allocatedSize;

        @JsonProperty("allocated-size-numeric")
        public long allocatedSizeNumeric;

        @JsonProperty("available-size")
        public String availableSize;

        @JsonProperty("available-size-numeric")
        public long availableSizeNumeric;

        @JsonProperty("affinity-size")
        public String affinitySize;

        @JsonProperty("affinity-size-numeric")
        public long affinitySizeNumeric;
    }
}
