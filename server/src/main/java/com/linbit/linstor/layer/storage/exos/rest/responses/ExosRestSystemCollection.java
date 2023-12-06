package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestSystemCollection extends ExosRestBaseResponse
{
    @JsonProperty("system")
    public ExosRestSystem[] system;

    public static class ExosRestSystem
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("system-name")
        public String systemName;

        @JsonProperty("system-contact")
        public String systemContact;

        @JsonProperty("system-location")
        public String systemLocation;

        @JsonProperty("system-information")
        public String systemInformation;

        @JsonProperty("midplane-serial-number")
        public String midplaneSerialNumber;

        @JsonProperty("vendor-name")
        public String vendorName;

        @JsonProperty("product-id")
        public String productId;

        @JsonProperty("product-brand")
        public String productBrand;

        @JsonProperty("scsi-vendor-id")
        public String scsiVendorId;

        @JsonProperty("scsi-product-id")
        public String scsiProductId;

        @JsonProperty("enclosure-count")
        public long enclosureCount;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("other-MC-status")
        public String otherMCStatus;

        @JsonProperty("other-MC-status-numeric")
        public long otherMCStatusNumeric;

        @JsonProperty("pfuStatus")
        public String pfuStatus;

        @JsonProperty("supported-locales")
        public String supportedLocales;

        @JsonProperty("current-node-wwn")
        public String currentNodeWwn;

        @JsonProperty("fde-security-status")
        public String fdeSecurityStatus;

        @JsonProperty("fde-security-status-numeric")
        public long fdeSecurityStatusNumeric;

        @JsonProperty("platform-type")
        public String platformType;

        @JsonProperty("platform-type-numeric")
        public long platformTypeNumeric;

        @JsonProperty("platform-brand")
        public String platformBrand;

        @JsonProperty("platform-brand-numeric")
        public long platformBrandNumeric;

        @JsonProperty("redundancy")
        public ExosRestRedundancy[] redundancy;

        @JsonProperty("unhealthy-component")
        public ExosRestUnhealthyComponent[] unhealthyComponent;
    }

    public static class ExosRestRedundancy
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("redundancy-mode")
        public String redundancyMode;

        @JsonProperty("redundancy-mode-numeric")
        public long redundancyModeNumeric;

        @JsonProperty("redundancy-status")
        public String redundancyStatus;

        @JsonProperty("redundancy-status-numeric")
        public long redundancyStatusNumeric;

        @JsonProperty("controller-a-status")
        public String controllerAStatus;

        @JsonProperty("controller-a-status-numeric")
        public long controllerAStatusNumeric;

        @JsonProperty("controller-a-serial-number")
        public String controllerASerialNumber;

        @JsonProperty("controller-b-status")
        public String controllerBStatus;

        @JsonProperty("controller-b-status-numeric")
        public long controllerBStatusNumeric;

        @JsonProperty("controller-b-serial-number")
        public String controllerBSerialNumber;

        @JsonProperty("other-MC-status")
        public String otherMCStatus;

        @JsonProperty("other-MC-status-numeric")
        public long otherMCStatusNumeric;
    }

    public static class ExosRestUnhealthyComponent
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("component-type")
        public String componentType;

        @JsonProperty("component-type-numeric")
        public long componentTypeNumeric;

        @JsonProperty("component-id")
        public String componentId;

        @JsonProperty("basetype")
        public String basetype;

        @JsonProperty("primary-key")
        public String primaryKey;

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
