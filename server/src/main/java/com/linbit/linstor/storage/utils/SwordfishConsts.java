package com.linbit.linstor.storage.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class SwordfishConsts
{
    public static final Pattern PATTERN_NQN = Pattern.compile(
        "^/sys/devices/virtual/nvme-fabrics/ctl/nvme(\\d+)/subsysnqn",
        Pattern.DOTALL | Pattern.MULTILINE
    );

    public static final Path SF_MAPPING_PATH = Paths.get("/", "var", "lib", "linstor", "swordfish.json");
    public static final Path SF_MAPPING_PATH_TMP = Paths.get("/", "var", "lib", "linstor", "swordfish.json.tmp");

    public static final String ODATA = "@odata";
    public static final String JSON_KEY_ODATA_ID = ODATA + ".id";
    public static final String JSON_KEY_MESSAGE = "Message";
    public static final String JSON_KEY_CAPACITY = "Capacity";
    public static final String JSON_KEY_CAPACITY_SOURCES = "CapacitySources";
    public static final String JSON_KEY_PROVIDING_POOLS = "ProvidingPools";
    public static final String JSON_KEY_NAME = "Name";
    public static final String JSON_KEY_CAPACITY_BYTES = "CapacityBytes";
    public static final String JSON_KEY_FREE_SIZE = "AvailableBytes";
    public static final String JSON_KEY_ALLOCATED_BYTES = "AllocatedBytes";
    public static final String JSON_KEY_GUARANTEED_BYTES = "GuaranteedBytes";
    public static final String JSON_KEY_RESOURCE = "Resource";
    public static final String JSON_KEY_DATA = "Data";
    public static final String JSON_KEY_IDENTIFIERS = "Identifiers";
    public static final String JSON_KEY_DURABLE_NAME_FORMAT = "DurableNameFormat";
    public static final String JSON_KEY_DURABLE_NAME = "DurableName";
    public static final Object JSON_KEY_LINKS = "Links";
    public static final Object JSON_KEY_OEM = "Oem";
    public static final Object JSON_KEY_INTEL_RACK_SCALE = "Intel_RackScale";
    public static final Object JSON_KEY_ENDPOINTS = "Endpoints";
    public static final Object JSON_KEY_PARAMETERS = "Parameters";
    public static final Object JSON_KEY_ALLOWABLE_VALUES = "AllowableValues";
    public static final String JSON_VALUE_DURABLE_NAME_FORMAT_SYSTEM_PATH = "SystemPath";
    public static final Object JSON_VALUE_NQN = "NQN";

    public static final String SF_BASE = "/redfish/v1";
    public static final String SF_STORAGE_SERVICES = "/StorageServices";
    public static final String SF_STORAGE_POOLS = "/StoragePools";
    public static final String SF_VOLUMES = "/Volumes";
    public static final String SF_NODES = "/Nodes";
    public static final String SF_ACTIONS = "/Actions";
    public static final String SF_ALLOCAT = "/Allocate";
    public static final String SF_FABRICS = "/Fabrics";
    public static final String SF_ENDPOINTS = "/Endpoints";
    public static final String SF_TASK_SERVICE = "/TaskService";
    public static final String SF_TASKS = "/Tasks";
    public static final String SF_MONITOR = "/Monitor";
    public static final String SF_METRICS = "/Metrics";
    public static final String SF_SYSTEMS = "/Systems";
    public static final String SF_ETHERNET_INTERFACES = "/EthernetInterfaces";
    public static final String SF_ZONES = "/Zones";
    public static final String SF_COMPOSED_NODE_ATTACH_RESOURCE = "/ComposedNode.AttachResource";
    public static final String SF_COMPOSED_NODE_DETACH_RESOURCE = "/ComposedNode.DetachResource";
    public static final String SF_ATTACH_RESOURCE_ACTION_INFO = "/AttachResourceActionInfo";

    public static final String SF_NODES_ACTION_ALLOCAT = SF_BASE + SF_NODES + SF_ACTIONS + SF_ALLOCAT;

    public static final long KIB = 1024;

    public static final String DRIVER_SF_VLM_ID_KEY = "sfVlmId";
    public static final String DRIVER_SF_STOR_SVC_ID_KEY = "sfStorSvcId";
}
