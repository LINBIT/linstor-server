package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestControllers extends ExosRestBaseResponse
{
    @JsonProperty("controllers")
    public @Nullable ExosRestController[] controllers;

    public static class ExosRestController
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("controller-id")
        public @Nullable String controllerId;

        @JsonProperty("controller-id-numeric")
        public long controllerIdNumeric;

        @JsonProperty("serial-number")
        public @Nullable String serialNumber;

        @JsonProperty("hardware-version")
        public @Nullable String hardwareVersion;

        @JsonProperty("cpld-version")
        public @Nullable String cpldVersion;

        @JsonProperty("mac-address")
        public @Nullable String macAddress;

        @JsonProperty("node-wwn")
        public @Nullable String nodeWwn;

        @JsonProperty("active-version")
        public long activeVersion;

        @JsonProperty("ip-address")
        public @Nullable String ipAddress;

        @JsonProperty("ip-subnet-mask")
        public @Nullable String ipSubnetMask;

        @JsonProperty("ip-gateway")
        public @Nullable String ipGateway;

        @JsonProperty("disks")
        public long disks;

        @JsonProperty("number-of-storage-pools")
        public long numberOfStoragePools;

        @JsonProperty("virtual-disks")
        public long virtualDisks;

        @JsonProperty("cache-memory-size")
        public long cacheMemorySize;

        @JsonProperty("system-memory-size")
        public long systemMemorySize;

        @JsonProperty("host-ports")
        public long hostPorts;

        @JsonProperty("drive-channels")
        public long driveChannels;

        @JsonProperty("drive-bus-type")
        public @Nullable String driveBusType;

        @JsonProperty("drive-bus-type-numeric")
        public long driveBusTypeNumeric;

        @JsonProperty("status")
        public @Nullable String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("failed-over")
        public @Nullable String failedOver;

        @JsonProperty("failed-over-numeric")
        public long failedOverNumeric;

        @JsonProperty("fail-over-reason")
        public @Nullable String failOverReason;

        @JsonProperty("fail-over-reason-numeric")
        public long failOverReasonNumeric;

        @JsonProperty("sc-fw")
        public @Nullable String scFw;

        @JsonProperty("vendor")
        public @Nullable String vendor;

        @JsonProperty("model")
        public @Nullable String model;

        @JsonProperty("platform-type")
        public @Nullable String platformType;

        @JsonProperty("platform-type-numeric")
        public long platformTypeNumeric;

        @JsonProperty("multicore")
        public @Nullable String multicore;

        @JsonProperty("multicore-numeric")
        public long multicoreNumeric;

        @JsonProperty("sc-cpu-type")
        public @Nullable String scCpuType;

        @JsonProperty("sc-cpu-speed")
        public long scCpuSpeed;

        @JsonProperty("internal-serial-number")
        public @Nullable String internalSerialNumber;

        @JsonProperty("cache-lock")
        public @Nullable String cacheLock;

        @JsonProperty("cache-lock-numeric")
        public long cacheLockNumeric;

        @JsonProperty("write-policy")
        public @Nullable String writePolicy;

        @JsonProperty("write-policy-numeric")
        public long writePolicyNumeric;

        @JsonProperty("description")
        public @Nullable String description;

        @JsonProperty("part-number")
        public @Nullable String partNumber;

        @JsonProperty("revision")
        public @Nullable String revision;

        @JsonProperty("dash-level")
        public @Nullable String dashLevel;

        @JsonProperty("fru-shortname")
        public @Nullable String fruShortname;

        @JsonProperty("mfg-date")
        public @Nullable String mfgDate;

        @JsonProperty("mfg-date-numeric")
        public long mfgDateNumeric;

        @JsonProperty("mfg-location")
        public @Nullable String mfgLocation;

        @JsonProperty("mfg-vendor-id")
        public @Nullable String mfgVendorId;

        @JsonProperty("locator-led")
        public @Nullable String locatorLed;

        @JsonProperty("locator-led-numeric")
        public long locatorLedNumeric;

        @JsonProperty("ssd-alt-path-io-count")
        public long ssdAltPathIoCount;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("health-recommendation")
        public @Nullable String healthRecommendation;

        @JsonProperty("position")
        public @Nullable String position;

        @JsonProperty("position-numeric")
        public long positionNumeric;

        @JsonProperty("rotation")
        public @Nullable String rotation;

        @JsonProperty("rotation-numeric")
        public long rotationNumeric;

        @JsonProperty("phy-isolation")
        public @Nullable String phyIsolation;

        @JsonProperty("phy-isolation-numeric")
        public long phyIsolationNumeric;

        @JsonProperty("redundancy-mode")
        public @Nullable String redundancyMode;

        @JsonProperty("redundancy-mode-numeric")
        public long redundancyModeNumeric;

        @JsonProperty("redundancy-status")
        public @Nullable String redundancyStatus;

        @JsonProperty("redundancy-status-numeric")
        public long redundancyStatusNumeric;

        @JsonProperty("network-parameters")
        public @Nullable ExosRestNetworkParameter[] networkParameters;

        @JsonProperty("port")
        public @Nullable ExosRestPort[] port;

        @JsonProperty("expander-ports")
        public @Nullable ExosRestExpanderPort[] expanderPorts;

        @JsonProperty("compact-flash")
        public @Nullable ExosRestCompactFlash[] compactFlash;

        @JsonProperty("expanders")
        public @Nullable ExosRestExpander[] expanders;
    }

    public static class ExosRestNetworkParameter
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("active-version")
        public long activeVersion;

        @JsonProperty("ip-address")
        public @Nullable String ipAddress;

        @JsonProperty("gateway")
        public @Nullable String gateway;

        @JsonProperty("subnet-mask")
        public @Nullable String subnetMask;

        @JsonProperty("mac-address")
        public @Nullable String macAddress;

        @JsonProperty("addressing-mode")
        public @Nullable String addressingMode;

        @JsonProperty("addressing-mode-numeric")
        public long addressingModeNumeric;

        @JsonProperty("link-speed")
        public @Nullable String linkSpeed;

        @JsonProperty("link-speed-numeric")
        public long linkSpeedNumeric;

        @JsonProperty("duplex-mode")
        public @Nullable String duplexMode;

        @JsonProperty("duplex-mode-numeric")
        public long duplexModeNumeric;

        @JsonProperty("auto-negotiation")
        public @Nullable String autoNegotiation;

        @JsonProperty("auto-negotiation-numeric")
        public long autoNegotiationNumeric;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("health-recommendation")
        public @Nullable String healthRecommendation;

        @JsonProperty("ping-broadcast")
        public @Nullable String pingBroadcast;

        @JsonProperty("ping-broadcast-numeric")
        public long pingBroadcastNumeric;
    }

    public static class ExosRestPort
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("controller")
        public @Nullable String controller;

        @JsonProperty("controller-numeric")
        public long controllerNumeric;

        @JsonProperty("port")
        public @Nullable String port;

        @JsonProperty("port-type")
        public @Nullable String portType;

        @JsonProperty("port-type-numeric")
        public long portTypeNumeric;

        @JsonProperty("media")
        public @Nullable String media;

        @JsonProperty("target-id")
        public @Nullable String targetId;

        @JsonProperty("status")
        public @Nullable String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("actual-speed")
        public @Nullable String actualSpeed;

        @JsonProperty("actual-speed-numeric")
        public long actualSpeedNumeric;

        @JsonProperty("configured-speed")
        public @Nullable String configuredSpeed;

        @JsonProperty("configured-speed-numeric")
        public long configuredSpeedNumeric;

        @JsonProperty("fan-out")
        public long fanOut;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("health-recommendation")
        public @Nullable String healthRecommendation;

        @JsonProperty("sas-port")
        public @Nullable ExosRestSasPort[] sasPort;
    }

    public static class ExosRestSasPort
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("configured-topology")
        public @Nullable String configuredTopology;

        @JsonProperty("configured-topology-numeric")
        public long configuredTopologyNumeric;

        @JsonProperty("width")
        public long width;

        @JsonProperty("sas-lanes-expected")
        public long sasLanesExpected;

        @JsonProperty("sas-active-lanes")
        public long sasActiveLanes;

        @JsonProperty("sas-disabled-lanes")
        public long sasDisabledLanes;
    }

    public static class ExosRestExpanderPort
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("enclosure-id")
        public long enclosureId;

        @JsonProperty("controller")
        public @Nullable String controller;

        @JsonProperty("controller-numeric")
        public long controllerNumeric;

        @JsonProperty("sas-port-type")
        public @Nullable String sasPortType;

        @JsonProperty("sas-port-type-numeric")
        public long sasPortTypeNumeric;

        @JsonProperty("sas-port-index")
        public long sasPortIndex;

        @JsonProperty("name")
        public @Nullable String name;

        @JsonProperty("status")
        public @Nullable String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("health-recommendation")
        public @Nullable String healthRecommendation;
    }

    public static class ExosRestCompactFlash
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("controller-id")
        public @Nullable String controllerId;

        @JsonProperty("controller-id-numeric")
        public long controllerIdNumeric;

        @JsonProperty("name")
        public @Nullable String name;

        @JsonProperty("status")
        public @Nullable String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("cache-flush")
        public @Nullable String cacheFlush;

        @JsonProperty("cache-flush-numeric")
        public long cacheFlushNumeric;

        @JsonProperty("health")
        public @Nullable String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public @Nullable String healthReason;

        @JsonProperty("health-recommendation")
        public @Nullable String healthRecommendation;
    }

    public static class ExosRestExpander
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("enclosure-id")
        public long enclosureId;

        @JsonProperty("drawer-id")
        public long drawerId;

        @JsonProperty("dom-id")
        public long domId;

        @JsonProperty("path-id")
        public @Nullable String pathId;

        @JsonProperty("path-id-numeric")
        public long pathIdNumeric;

        @JsonProperty("name")
        public @Nullable String name;

        @JsonProperty("location")
        public @Nullable String location;

        @JsonProperty("status")
        public @Nullable String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("extended-status")
        public @Nullable String extendedStatus;

        @JsonProperty("fw-revision")
        public @Nullable String fwRevision;

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
