package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestSystemCollection extends ExosRestBaseResponse
{
    @JsonProperty("system")
    public @Nullable ExosRestSystem[] system;

    public static class ExosRestSystem
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("system-name")
        public @Nullable String systemName;

        @JsonProperty("system-contact")
        public @Nullable String systemContact;

        @JsonProperty("system-location")
        public @Nullable String systemLocation;

        @JsonProperty("system-information")
        public @Nullable String systemInformation;

        @JsonProperty("midplane-serial-number")
        public @Nullable String midplaneSerialNumber;

        @JsonProperty("vendor-name")
        public @Nullable String vendorName;

        @JsonProperty("product-id")
        public @Nullable String productId;

        @JsonProperty("product-brand")
        public @Nullable String productBrand;

        @JsonProperty("scsi-vendor-id")
        public @Nullable String scsiVendorId;

        @JsonProperty("scsi-product-id")
        public @Nullable String scsiProductId;

        @JsonProperty("enclosure-count")
        public long enclosureCount;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("other-MC-status")
        public @Nullable String otherMCStatus;

        @JsonProperty("other-MC-status-numeric")
        public long otherMCStatusNumeric;

        @JsonProperty("pfuStatus")
        public @Nullable String pfuStatus;

        @JsonProperty("supported-locales")
        public @Nullable String supportedLocales;

        @JsonProperty("current-node-wwn")
        public @Nullable String currentNodeWwn;

        @JsonProperty("fde-security-status")
        public @Nullable String fdeSecurityStatus;

        @JsonProperty("fde-security-status-numeric")
        public long fdeSecurityStatusNumeric;

        @JsonProperty("platform-type")
        public @Nullable String platformType;

        @JsonProperty("platform-type-numeric")
        public long platformTypeNumeric;

        @JsonProperty("platform-brand")
        public @Nullable String platformBrand;

        @JsonProperty("platform-brand-numeric")
        public long platformBrandNumeric;

        @JsonProperty("redundancy")
        public @Nullable ExosRestRedundancy[] redundancy;

        @JsonProperty("unhealthy-component")
        public @Nullable ExosRestUnhealthyComponent[] unhealthyComponent;
    }

    public static class ExosRestRedundancy
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("redundancy-mode")
        public @Nullable String redundancyMode;

        @JsonProperty("redundancy-mode-numeric")
        public long redundancyModeNumeric;

        @JsonProperty("redundancy-status")
        public @Nullable String redundancyStatus;

        @JsonProperty("redundancy-status-numeric")
        public long redundancyStatusNumeric;

        @JsonProperty("controller-a-status")
        public @Nullable String controllerAStatus;

        @JsonProperty("controller-a-status-numeric")
        public long controllerAStatusNumeric;

        @JsonProperty("controller-a-serial-number")
        public @Nullable String controllerASerialNumber;

        @JsonProperty("controller-b-status")
        public @Nullable String controllerBStatus;

        @JsonProperty("controller-b-status-numeric")
        public long controllerBStatusNumeric;

        @JsonProperty("controller-b-serial-number")
        public @Nullable String controllerBSerialNumber;

        @JsonProperty("other-MC-status")
        public @Nullable String otherMCStatus;

        @JsonProperty("other-MC-status-numeric")
        public long otherMCStatusNumeric;
    }

    public static class ExosRestUnhealthyComponent
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("component-type")
        public @Nullable String componentType;

        @JsonProperty("component-type-numeric")
        public long componentTypeNumeric;

        @JsonProperty("component-id")
        public @Nullable String componentId;

        @JsonProperty("basetype")
        public @Nullable String basetype;

        @JsonProperty("primary-key")
        public @Nullable String primaryKey;

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
