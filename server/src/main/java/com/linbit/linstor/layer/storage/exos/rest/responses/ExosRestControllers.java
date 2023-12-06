package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestControllers extends ExosRestBaseResponse
{
    @JsonProperty("controllers")
    public ExosRestController[] controllers;

    public static class ExosRestController
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("controller-id")
        public String controllerId;

        @JsonProperty("controller-id-numeric")
        public long controllerIdNumeric;

        @JsonProperty("serial-number")
        public String serialNumber;

        @JsonProperty("hardware-version")
        public String hardwareVersion;

        @JsonProperty("cpld-version")
        public String cpldVersion;

        @JsonProperty("mac-address")
        public String macAddress;

        @JsonProperty("node-wwn")
        public String nodeWwn;

        @JsonProperty("active-version")
        public long activeVersion;

        @JsonProperty("ip-address")
        public String ipAddress;

        @JsonProperty("ip-subnet-mask")
        public String ipSubnetMask;

        @JsonProperty("ip-gateway")
        public String ipGateway;

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
        public String driveBusType;

        @JsonProperty("drive-bus-type-numeric")
        public long driveBusTypeNumeric;

        @JsonProperty("status")
        public String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("failed-over")
        public String failedOver;

        @JsonProperty("failed-over-numeric")
        public long failedOverNumeric;

        @JsonProperty("fail-over-reason")
        public String failOverReason;

        @JsonProperty("fail-over-reason-numeric")
        public long failOverReasonNumeric;

        @JsonProperty("sc-fw")
        public String scFw;

        @JsonProperty("vendor")
        public String vendor;

        @JsonProperty("model")
        public String model;

        @JsonProperty("platform-type")
        public String platformType;

        @JsonProperty("platform-type-numeric")
        public long platformTypeNumeric;

        @JsonProperty("multicore")
        public String multicore;

        @JsonProperty("multicore-numeric")
        public long multicoreNumeric;

        @JsonProperty("sc-cpu-type")
        public String scCpuType;

        @JsonProperty("sc-cpu-speed")
        public long scCpuSpeed;

        @JsonProperty("internal-serial-number")
        public String internalSerialNumber;

        @JsonProperty("cache-lock")
        public String cacheLock;

        @JsonProperty("cache-lock-numeric")
        public long cacheLockNumeric;

        @JsonProperty("write-policy")
        public String writePolicy;

        @JsonProperty("write-policy-numeric")
        public long writePolicyNumeric;

        @JsonProperty("description")
        public String description;

        @JsonProperty("part-number")
        public String partNumber;

        @JsonProperty("revision")
        public String revision;

        @JsonProperty("dash-level")
        public String dashLevel;

        @JsonProperty("fru-shortname")
        public String fruShortname;

        @JsonProperty("mfg-date")
        public String mfgDate;

        @JsonProperty("mfg-date-numeric")
        public long mfgDateNumeric;

        @JsonProperty("mfg-location")
        public String mfgLocation;

        @JsonProperty("mfg-vendor-id")
        public String mfgVendorId;

        @JsonProperty("locator-led")
        public String locatorLed;

        @JsonProperty("locator-led-numeric")
        public long locatorLedNumeric;

        @JsonProperty("ssd-alt-path-io-count")
        public long ssdAltPathIoCount;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("health-recommendation")
        public String healthRecommendation;

        @JsonProperty("position")
        public String position;

        @JsonProperty("position-numeric")
        public long positionNumeric;

        @JsonProperty("rotation")
        public String rotation;

        @JsonProperty("rotation-numeric")
        public long rotationNumeric;

        @JsonProperty("phy-isolation")
        public String phyIsolation;

        @JsonProperty("phy-isolation-numeric")
        public long phyIsolationNumeric;

        @JsonProperty("redundancy-mode")
        public String redundancyMode;

        @JsonProperty("redundancy-mode-numeric")
        public long redundancyModeNumeric;

        @JsonProperty("redundancy-status")
        public String redundancyStatus;

        @JsonProperty("redundancy-status-numeric")
        public long redundancyStatusNumeric;

        @JsonProperty("network-parameters")
        public ExosRestNetworkParameter[] networkParameters;

        @JsonProperty("port")
        public ExosRestPort[] port;

        @JsonProperty("expander-ports")
        public ExosRestExpanderPort[] expanderPorts;

        @JsonProperty("compact-flash")
        public ExosRestCompactFlash[] compactFlash;

        @JsonProperty("expanders")
        public ExosRestExpander[] expanders;
    }

    public static class ExosRestNetworkParameter
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("active-version")
        public long activeVersion;

        @JsonProperty("ip-address")
        public String ipAddress;

        @JsonProperty("gateway")
        public String gateway;

        @JsonProperty("subnet-mask")
        public String subnetMask;

        @JsonProperty("mac-address")
        public String macAddress;

        @JsonProperty("addressing-mode")
        public String addressingMode;

        @JsonProperty("addressing-mode-numeric")
        public long addressingModeNumeric;

        @JsonProperty("link-speed")
        public String linkSpeed;

        @JsonProperty("link-speed-numeric")
        public long linkSpeedNumeric;

        @JsonProperty("duplex-mode")
        public String duplexMode;

        @JsonProperty("duplex-mode-numeric")
        public long duplexModeNumeric;

        @JsonProperty("auto-negotiation")
        public String autoNegotiation;

        @JsonProperty("auto-negotiation-numeric")
        public long autoNegotiationNumeric;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("health-recommendation")
        public String healthRecommendation;

        @JsonProperty("ping-broadcast")
        public String pingBroadcast;

        @JsonProperty("ping-broadcast-numeric")
        public long pingBroadcastNumeric;
    }

    public static class ExosRestPort
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("controller")
        public String controller;

        @JsonProperty("controller-numeric")
        public long controllerNumeric;

        @JsonProperty("port")
        public String port;

        @JsonProperty("port-type")
        public String portType;

        @JsonProperty("port-type-numeric")
        public long portTypeNumeric;

        @JsonProperty("media")
        public String media;

        @JsonProperty("target-id")
        public String targetId;

        @JsonProperty("status")
        public String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("actual-speed")
        public String actualSpeed;

        @JsonProperty("actual-speed-numeric")
        public long actualSpeedNumeric;

        @JsonProperty("configured-speed")
        public String configuredSpeed;

        @JsonProperty("configured-speed-numeric")
        public long configuredSpeedNumeric;

        @JsonProperty("fan-out")
        public long fanOut;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("health-recommendation")
        public String healthRecommendation;

        @JsonProperty("sas-port")
        public ExosRestSasPort[] sasPort;
    }

    public static class ExosRestSasPort
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("configured-topology")
        public String configuredTopology;

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
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("enclosure-id")
        public long enclosureId;

        @JsonProperty("controller")
        public String controller;

        @JsonProperty("controller-numeric")
        public long controllerNumeric;

        @JsonProperty("sas-port-type")
        public String sasPortType;

        @JsonProperty("sas-port-type-numeric")
        public long sasPortTypeNumeric;

        @JsonProperty("sas-port-index")
        public long sasPortIndex;

        @JsonProperty("name")
        public String name;

        @JsonProperty("status")
        public String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("health-recommendation")
        public String healthRecommendation;
    }

    public static class ExosRestCompactFlash
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("controller-id")
        public String controllerId;

        @JsonProperty("controller-id-numeric")
        public long controllerIdNumeric;

        @JsonProperty("name")
        public String name;

        @JsonProperty("status")
        public String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("cache-flush")
        public String cacheFlush;

        @JsonProperty("cache-flush-numeric")
        public long cacheFlushNumeric;

        @JsonProperty("health")
        public String health;

        @JsonProperty("health-numeric")
        public long healthNumeric;

        @JsonProperty("health-reason")
        public String healthReason;

        @JsonProperty("health-recommendation")
        public String healthRecommendation;
    }

    public static class ExosRestExpander
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("enclosure-id")
        public long enclosureId;

        @JsonProperty("drawer-id")
        public long drawerId;

        @JsonProperty("dom-id")
        public long domId;

        @JsonProperty("path-id")
        public String pathId;

        @JsonProperty("path-id-numeric")
        public long pathIdNumeric;

        @JsonProperty("name")
        public String name;

        @JsonProperty("location")
        public String location;

        @JsonProperty("status")
        public String status;

        @JsonProperty("status-numeric")
        public long statusNumeric;

        @JsonProperty("extended-status")
        public String extendedStatus;

        @JsonProperty("fw-revision")
        public String fwRevision;

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
