/**
This file was generated with rest-gen.py, do not modify directly, the chances are high it is useless.
**/

package com.linbit.linstor.api.rest.v1.serializer;

import java.util.List;
import java.util.Map;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonInclude;

public class JsonGenTypes
{
    public static final String REST_API_VERSION = "1.0.12";

    /**
     * Common api reply structure
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ApiCallRc
    {
        /**
         * A masked error number
         */
        public Long ret_code;
        public String message;
        /**
         * Cause of the error
         */
        public String cause;
        /**
         * Details to the error message
         */
        public String details;
        /**
         * Possible correction options
         */
        public String correction;
        /**
         * List of error report ids related to this api call return code.
         */
        public List<String> error_report_ids = Collections.emptyList();
        /**
         * Map of objection that have been involved by the operation.
         */
        public Map<String, String> obj_refs = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Node
    {
        public String name;
        public String type;
        public List<String> flags = Collections.emptyList();
        public Map<String, String> props = Collections.emptyMap();
        public List<NetInterface> net_interfaces = Collections.emptyList();
        /**
         * Enum describing the current connection status.
         */
        public String connection_status;
        /**
         * unique object id
         */
        public String uuid;
        public List<String> storage_providers = Collections.emptyList();
        public List<String> resource_layers = Collections.emptyList();
        public Map<String, List<String>> unsupported_providers = Collections.emptyMap();
        public Map<String, List<String>> unsupported_layers = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeModify
    {
        public String node_type;
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NetInterface
    {
        public String name;
        public String address;
        public Integer satellite_port;
        public String satellite_encryption_type;
        /**
         * Defines if this netinterface should be used for the satellite connection
         */
        public Boolean is_active;
        /**
         * unique object id
         */
        public String uuid;
    }

//    /**
//     * A string to string property map.
//     */
//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class Properties
//    {
//    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class ProviderKind
//    {
//    }

    /**
     * Contains information about a storage pool.
     *
     * If state is `Error` check the storage pool object path for detailed error description:
     *
     *   /v1/nodes/{nodename}/storage-pools/{poolname}
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StoragePool
    {
        public String storage_pool_name;
        public String node_name;
        public String provider_kind;
        public Map<String, String> props = Collections.emptyMap();
        /**
         * read only map of static storage pool traits
         */
        public Map<String, String> static_traits = Collections.emptyMap();
        /**
         * Kibi - read only
         */
        public Long free_capacity;
        /**
         * Kibi - read only
         */
        public Long total_capacity;
        /**
         * read only
         */
        public String free_space_mgr_name;
        /**
         * unique object id
         */
        public String uuid;
        /**
         * Currently known report messages for this storage pool
         */
        public List<ApiCallRc> reports = Collections.emptyList();
        /**
         * true if the storage pool supports snapshots. false otherwise
         */
        public Boolean supports_snapshots;
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class LayerType
//    {
//    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdResourceDefinitionLayer
    {
        public String resource_name_suffix;
        public Integer peer_slots;
        public Integer al_stripes;
        public Long al_stripe_size_kib;
        /**
         * used drbd port for this resource
         */
        public Integer port;
        public String transport_type;
        /**
         * drbd resource secret
         */
        public String secret;
        public Boolean down;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionLayer
    {
        public String type;
        public Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinition
    {
        public String name = "";
        /**
         * External name can be used to have native resource names.
         * If you need to store a non Linstor compatible resource name use this field
         * and Linstor will generate a compatible name.
         */
        public String external_name;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<ResourceDefinitionLayer> layer_data = Collections.emptyList();
        /**
         * unique object id
         */
        public String uuid;
        /**
         * name of the linked resource group, if there is a link
         */
        public String resource_group_name;
        public List<VolumeDefinition> volume_definitions = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionCreate
    {
        /**
         * drbd port for resources
         */
        public Integer drbd_port;
        /**
         * drbd resource secret
         */
        public String drbd_secret;
        /**
         * drbd peer slot number
         */
        public Integer drbd_peer_slots;
        public String drbd_transport_type;
        public ResourceDefinition resource_definition = new ResourceDefinition();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
        /**
         * drbd port for resources
         */
        public Integer drbd_port;
        /**
         * drbd peer slot number
         */
        public Integer drbd_peer_slots;
        public List<String> layer_stack = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdVolumeDefinition
    {
        public String resource_name_suffix;
        public Integer volume_number;
        public Integer minor_number;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionLayer
    {
        public String type;
        public Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinition
    {
        public Integer volume_number;
        /**
         * Size of the volume in Kibi.
         */
        public Long size_kib;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<VolumeDefinitionLayer> layer_data = Collections.emptyList();
        /**
         * unique object id
         */
        public String uuid;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionCreate
    {
        public VolumeDefinition volume_definition = new VolumeDefinition();
        public Integer drbd_minor_number;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionModify
    {
        public Long size_kib;
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdResource
    {
        public DrbdResourceDefinitionLayer drbd_resource_definition;
        public Integer node_id;
        public Integer peer_slots;
        public Integer al_stripes;
        public Long al_size;
        public List<String> flags = Collections.emptyList();
        public List<DrbdVolume> drbd_volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorageResource
    {
        public List<StorageVolume> storage_volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class LUKSResource
    {
        public List<LUKSVolume> luks_volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NVMEResource
    {
        public List<NVMEVolume> nvme_volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class WritecacheResource
    {
        public List<WritecacheVolume> writecache_volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceLayer
    {
        public List<ResourceLayer> children = Collections.emptyList();
        public String resource_name_suffix;
        public String type;
        public DrbdResource drbd;
        public LUKSResource luks;
        public StorageResource storage;
        public NVMEResource nvme;
        public WritecacheResource writecache;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceState
    {
        public Boolean in_use;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Resource
    {
        public String name;
        public String node_name;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public ResourceLayer layer_object;
        public ResourceState state;
        /**
         * unique object id
         */
        public String uuid;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceWithVolumes
        extends Resource
    {
        public List<Volume> volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceCreate
    {
        public Resource resource;
        public List<String> layer_list = Collections.emptyList();
        public Integer drbd_node_id;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdVolume
    {
        public DrbdVolumeDefinition drbd_volume_definition;
        /**
         * drbd device path e.g. '/dev/drbd1000'
         */
        public String device_path;
        /**
         * block device used by drbd
         */
        public String backing_device;
        public String meta_disk;
        public Long allocated_size_kib;
        public Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public String disk_state;
        /**
         * Storage pool name used for external meta data; null for internal
         */
        public String ext_meta_stor_pool;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class LUKSVolume
    {
        public Integer volume_number;
        /**
         * block device path
         */
        public String device_path;
        /**
         * block device used by luks
         */
        public String backing_device;
        public Long allocated_size_kib;
        public Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public String disk_state;
        public Boolean opened;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorageVolume
    {
        public Integer volume_number;
        /**
         * block device path
         */
        public String device_path;
        public Long allocated_size_kib;
        public Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NVMEVolume
    {
        public Integer volume_number;
        /**
         * block device path
         */
        public String device_path;
        /**
         * block device used by nvme
         */
        public String backing_device;
        public Long allocated_size_kib;
        public Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class WritecacheVolume
    {
        public Integer volume_number;
        /**
         * block device path
         */
        public String device_path;
        /**
         * block device path used as cache device
         */
        public String device_path_cache;
        public Long allocated_size_kib;
        public Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeLayer
    {
        public String type;
        public Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Volume
    {
        public Integer volume_number;
        public String storage_pool_name;
        public String provider_kind;
        public String device_path;
        public Long allocated_size_kib;
        public Long usable_size_kib;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public VolumeState state;
        public List<VolumeLayer> layer_data_list = Collections.emptyList();
        /**
         * unique object id
         */
        public String uuid;
        public List<ApiCallRc> reports = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeState
    {
        public String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroup
    {
        public String name = "";
        public String description = "";
        public Map<String, String> props = Collections.emptyMap();
        public AutoSelectFilter select_filter;
        /**
         * unique object id
         */
        public String uuid;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroupModify
    {
        public String description;
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
        public AutoSelectFilter select_filter;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroupSpawn
    {
        /**
         * name of the resulting resource-definition
         */
        public String resource_definition_name;
        /**
         * sizes (in kib) of the resulting volume-definitions
         */
        public List<Long> volume_sizes = Collections.emptyList();
        /**
         * If false, the length of the vlm_sizes has to match the number of volume-groups or an
         * error is returned.
         *
         * If true and there are more vlm_sizes than volume-groups, the additional volume-definitions
         * will simply have no pre-set properties (i.e. "empty" volume-definitions)
         * If true and there are less vlm_sizes than volume-groups, the additional volume-groups
         * won't be used.
         *
         * If the count of vlm_sizes matches the number of volume-groups, this "partial" parameter
         * has no effect.
         */
        public boolean partial = false;
        /**
         * If true, the spawn command will only create the resource-definition with the volume-definitions
         * but will not perform an auto-place, even if it is configured.
         */
        public boolean definitions_only = false;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeGroup
    {
        public Integer volume_number;
        public Map<String, String> props = Collections.emptyMap();
        /**
         * unique object id
         */
        public String uuid;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeGroupModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AutoPlaceRequest
    {
        public boolean diskless_on_remaining = false;
        public AutoSelectFilter select_filter = new AutoSelectFilter();
        public List<String> layer_list = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AutoSelectFilter
    {
        public Integer place_count;
        public String storage_pool;
        public List<String> not_place_with_rsc = Collections.emptyList();
        public String not_place_with_rsc_regex;
        public List<String> replicas_on_same = Collections.emptyList();
        public List<String> replicas_on_different = Collections.emptyList();
        public List<String> layer_stack = Collections.emptyList();
        public List<String> provider_list = Collections.emptyList();
        public Boolean diskless_on_remaining;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Candidate
    {
        public String storage_pool;
        /**
         * maximum size in KiB
         */
        public Long max_volume_size_kib;
        public List<String> node_names = Collections.emptyList();
        public Boolean all_thin;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class MaxVolumeSizes
    {
        public List<Candidate> candidates = Collections.emptyList();
        public Double default_max_oversubscription_ratio;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerPropsModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerVersion
    {
        public String version;
        public String git_hash;
        public String build_time;
        public String rest_api_version;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ErrorReport
    {
        public String node_name;
        public Long error_time;
        /**
         * Filename of the error report on the server.
         *
         * Format is:
         * ```ErrorReport-{instanceid}-{nodeid}-{squencenumber}.log```
         */
        public String filename;
        /**
         * Contains the full text of the error report file.
         */
        public String text;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class KeyValueStore
    {
        /**
         * name of the key value store
         */
        public String name;
        public Map<String, String> props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class KeyValueStoreModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceConnection
    {
        /**
         * source node of the connection
         */
        public String node_a;
        /**
         * target node of the connection
         */
        public String node_b;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public Integer port;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceConnectionModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Snapshot
    {
        public String name;
        public String resource_name;
        public List<String> nodes = Collections.emptyList();
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<SnapshotVolumeDefinition> volume_definitions = Collections.emptyList();
        /**
         * unique object id
         */
        public String uuid;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotVolumeDefinition
    {
        public Integer volume_number;
        /**
         * Volume size in KiB
         */
        public Long size_kib;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotRestore
    {
        /**
         * Resource where to restore the snapshot
         */
        public String to_resource;
        /**
         * List of nodes where to place the restored snapshot
         */
        public List<String> nodes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PassPhraseCreate
    {
        public String new_passphrase;
        public String old_passphrase;
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class PassPhraseEnter
//    {
//    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StoragePoolDefinition
    {
        public String storage_pool_name;
        public Map<String, String> props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StoragePoolDefinitionModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdProxyEnable
    {
        /**
         * Proxy port to use (optional)
         */
        public Integer port;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdProxyModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        /**
         * Compression type used by the proxy.
         */
        public String compression_type;
        public Map<String, String> compression_props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorage
    {
        public Long size;
        public Boolean rotational;
        public Map<String, List<PhysicalStorageDevice>> nodes = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorageDevice
    {
        public String device;
        public String model;
        public String serial;
        public String wwn;
    }

    /**
     * This structured is used for create physical-storage
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorageStoragePoolCreate
    {
        /**
         * Name of the linstor storage pool
         */
        public String name;
        public Map<String, String> props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorageCreate
    {
        public String provider_kind;
        public List<String> device_paths = Collections.emptyList();
        /**
         * RAID level to use for pool.
         */
        public String raid_level = "JBOD";
        public String pool_name;
        public boolean vdo_enable = false;
        public int vdo_slab_size_kib = 0;
        public int vdo_logical_size_kib = 0;
        /**
         * If specified a linstor storage pool will also be created using this device pool
         */
        public PhysicalStorageStoragePoolCreate with_storage_pool;
    }

    private JsonGenTypes()
    {
    }
}
