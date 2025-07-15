/**
This file was generated with rest-gen.py, do not modify directly, the chances are high it is useless.
**/

package com.linbit.linstor.api.rest.v1.serializer;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonInclude;

public class JsonGenTypes
{
    public static final String REST_API_VERSION = "1.26.0";

    /**
     * Common api reply structure
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ApiCallRc
    {
        /**
         * A masked error number
         */
        public long ret_code;
        public String message;
        /**
         * Cause of the error
         */
        public @Nullable String cause;
        /**
         * Details to the error message
         */
        public @Nullable String details;
        /**
         * Possible correction options
         */
        public @Nullable String correction;
        /**
         * List of error report ids related to this api call return code.
         */
        public List<String> error_report_ids = Collections.emptyList();
        /**
         * Map of objects that have been involved in the operation.
         */
        public Map<String, String> obj_refs = Collections.emptyMap();
        public @Nullable String created_at;
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class ApiCallRcList
//    {
//    }

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
        public @Nullable String connection_status;
        /**
         * unique object id
         */
        public @Nullable String uuid;
        public List<String> storage_providers = Collections.emptyList();
        public List<String> resource_layers = Collections.emptyList();
        public Map<String, List<String>> unsupported_providers = Collections.emptyMap();
        public Map<String, List<String>> unsupported_layers = Collections.emptyMap();
        /**
         * milliseconds since unix epoch in UTC
         */
        public @Nullable Long eviction_timestamp;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeCreateEbs
    {
        public String name;
        public String ebs_remote_name;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeModify
    {
        public @Nullable String node_type;
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeRestore
    {
        public @Nullable Boolean delete_resources;
        public @Nullable Boolean delete_snapshots;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NetInterface
    {
        public String name;
        public String address;
        public @Nullable Integer satellite_port;
        public @Nullable String satellite_encryption_type;
        /**
         * Defines if this netinterface should be used for the satellite connection
         */
        public @Nullable Boolean is_active;
        /**
         * unique object id
         */
        public @Nullable String uuid;
    }

//    /**
//     * A string to string property map.
//     */
//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class Properties
//    {
//    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PropertyWithDescription
    {
        public String type;
        public String value;
        public @Nullable String descr;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class EffectivePropertiesMapValue
        extends PropertyWithDescription
    {
        public List<PropertyWithDescription> other = Collections.emptyList();
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class EffectivePropertiesMap
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
        public @Nullable String node_name;
        public String provider_kind;
        public Map<String, String> props = Collections.emptyMap();
        /**
         * read only map of static storage pool traits
         */
        public Map<String, String> static_traits = Collections.emptyMap();
        /**
         * Kibi - read only
         */
        public @Nullable Long free_capacity;
        /**
         * Kibi - read only
         */
        public @Nullable Long total_capacity;
        /**
         * read only
         */
        public @Nullable String free_space_mgr_name;
        /**
         * unique object id
         */
        public @Nullable String uuid;
        /**
         * Currently known report messages for this storage pool
         */
        public List<ApiCallRc> reports = Collections.emptyList();
        /**
         * true if the storage pool supports snapshots. false otherwise
         */
        public @Nullable Boolean supports_snapshots;
        /**
         * Name of the shared space or null if none given
         */
        public @Nullable String shared_space;
        /**
         * true if a shared storage pool uses linstor-external locking, like cLVM
         */
        public boolean external_locking = false;
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class LayerType
//    {
//    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdResourceDefinitionLayer
    {
        public @Nullable String resource_name_suffix;
        public @Nullable Integer peer_slots;
        public @Nullable Integer al_stripes;
        public @Nullable Long al_stripe_size_kib;
        /**
         * used drbd port for this resource
         */
        public @Nullable Integer port;
        public @Nullable String transport_type;
        /**
         * drbd resource secret
         */
        public @Nullable String secret;
        public @Nullable Boolean down;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionLayer
    {
        public @Nullable String type;
        public @Nullable Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinition
    {
        public String name = "";
        public @Nullable String external_name;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<ResourceDefinitionLayer> layer_data = Collections.emptyList();
        /**
         * unique object id
         */
        public @Nullable String uuid;
        /**
         * name of the linked resource group, if there is a link
         */
        public @Nullable String resource_group_name;
        public List<VolumeDefinition> volume_definitions = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionCreate
    {
        /**
         * drbd port for resources
         */
        public @Nullable Integer drbd_port;
        /**
         * drbd resource secret
         */
        public @Nullable String drbd_secret;
        /**
         * drbd peer slot number
         */
        public @Nullable Integer drbd_peer_slots;
        public @Nullable String drbd_transport_type;
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
        public @Nullable Integer drbd_port;
        /**
         * drbd peer slot number
         */
        public @Nullable Integer drbd_peer_slots;
        public List<String> layer_stack = Collections.emptyList();
        /**
         * change resource group to the given group name
         */
        public @Nullable String resource_group;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdVolumeDefinition
    {
        public @Nullable String resource_name_suffix;
        public @Nullable Integer volume_number;
        public @Nullable Integer minor_number;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionLayer
    {
        public String type;
        public @Nullable Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinition
    {
        public @Nullable Integer volume_number;
        /**
         * Size of the volume in Kibi.
         */
        public long size_kib;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<VolumeDefinitionLayer> layer_data = Collections.emptyList();
        /**
         * unique object id
         */
        public @Nullable String uuid;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionCreate
    {
        public VolumeDefinition volume_definition = new VolumeDefinition();
        public @Nullable Integer drbd_minor_number;
        /**
         * optional passphrase for encrypted volumes
         */
        public @Nullable String passphrase;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionModify
    {
        public @Nullable Long size_kib;
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
        /**
         * To add a flag just specify the flag name, to remove a flag prepend it with a '-'.
         *
         * Flags:
         *   * GROSS_SIZE
         */
        public List<String> flags = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeDefinitionModifyPassphrase
    {
        public String new_passphrase;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdResource
    {
        public @Nullable DrbdResourceDefinitionLayer drbd_resource_definition;
        public @Nullable Integer node_id;
        public @Nullable Integer peer_slots;
        public @Nullable Integer al_stripes;
        public @Nullable Long al_size;
        public List<String> flags = Collections.emptyList();
        public List<DrbdVolume> drbd_volumes = Collections.emptyList();
        public Map<String, DrbdConnection> connections = Collections.emptyMap();
        public @Nullable Integer promotion_score;
        public @Nullable Boolean may_promote;
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
    public static class CacheResource
    {
        public List<CacheVolume> cache_volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BCacheResource
    {
        public List<BCacheVolume> bcache_volumes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceLayer
    {
        public List<ResourceLayer> children = Collections.emptyList();
        public @Nullable String resource_name_suffix;
        public @Nullable String type;
        public @Nullable DrbdResource drbd;
        public @Nullable LUKSResource luks;
        public @Nullable StorageResource storage;
        public @Nullable NVMEResource nvme;
        public @Nullable WritecacheResource writecache;
        public @Nullable CacheResource cache;
        public @Nullable BCacheResource bcache;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceState
    {
        public @Nullable Boolean in_use;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdConnection
    {
        public @Nullable Boolean connected;
        /**
         * DRBD connection status
         */
        public @Nullable String message;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Resource
    {
        public @Nullable String name;
        public @Nullable String node_name;
        public Map<String, String> props = Collections.emptyMap();
        public Map<String, EffectivePropertiesMapValue> effective_props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public @Nullable ResourceLayer layer_object;
        public @Nullable ResourceState state;
        /**
         * unique object id
         */
        public @Nullable String uuid;
        /**
         * milliseconds since unix epoch in UTC
         */
        public @Nullable Long create_timestamp;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceWithVolumes
        extends Resource
    {
        public List<Volume> volumes = Collections.emptyList();
        /**
         * shared space name of the data storage pool of the first volume of
         * the resource or empty if data storage pool is not shared
         */
        public @Nullable String shared_name;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceCreate
    {
        public @Nullable Resource resource;
        public List<String> layer_list = Collections.emptyList();
        public @Nullable Integer drbd_node_id;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceMakeAvailable
    {
        public List<String> layer_list = Collections.emptyList();
        /**
         * if true resource will be created as diskful even if diskless would be possible
         */
        public boolean diskful = false;
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
        public @Nullable DrbdVolumeDefinition drbd_volume_definition;
        /**
         * drbd device path e.g. '/dev/drbd1000'
         */
        public @Nullable String device_path;
        /**
         * block device used by drbd
         */
        public @Nullable String backing_device;
        public @Nullable String meta_disk;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public @Nullable String disk_state;
        /**
         * Storage pool name used for external meta data; null for internal
         */
        public @Nullable String ext_meta_stor_pool;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class LUKSVolume
    {
        public @Nullable Integer volume_number;
        /**
         * block device path
         */
        public @Nullable String device_path;
        /**
         * block device used by luks
         */
        public @Nullable String backing_device;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public @Nullable String disk_state;
        public @Nullable Boolean opened;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorageVolume
    {
        public @Nullable Integer volume_number;
        /**
         * block device path
         */
        public @Nullable String device_path;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public @Nullable String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NVMEVolume
    {
        public @Nullable Integer volume_number;
        /**
         * block device path
         */
        public @Nullable String device_path;
        /**
         * block device used by nvme
         */
        public @Nullable String backing_device;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public @Nullable String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class WritecacheVolume
    {
        public @Nullable Integer volume_number;
        /**
         * block device path
         */
        public @Nullable String device_path;
        /**
         * block device path used as cache device
         */
        public @Nullable String device_path_cache;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public @Nullable String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class CacheVolume
    {
        public @Nullable Integer volume_number;
        /**
         * block device path
         */
        public @Nullable String device_path;
        /**
         * block device path used as cache device
         */
        public @Nullable String device_path_cache;
        /**
         * block device path used as meta device
         */
        public @Nullable String device_meta_cache;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public @Nullable String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BCacheVolume
    {
        public @Nullable Integer volume_number;
        /**
         * block device path
         */
        public @Nullable String device_path;
        /**
         * block device path used as cache device
         */
        public @Nullable String device_path_cache;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        /**
         * String describing current volume state
         */
        public @Nullable String disk_state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeLayer
    {
        public @Nullable String type;
        public @Nullable Object data;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Volume
    {
        public @Nullable Integer volume_number;
        public @Nullable String storage_pool_name;
        public @Nullable String provider_kind;
        public @Nullable String device_path;
        public @Nullable Long allocated_size_kib;
        public @Nullable Long usable_size_kib;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public @Nullable VolumeState state;
        public List<VolumeLayer> layer_data_list = Collections.emptyList();
        /**
         * unique object id
         */
        public @Nullable String uuid;
        public List<ApiCallRc> reports = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeState
    {
        public @Nullable String disk_state;
        public Map<String, ReplicationState> replication_states = Collections.emptyMap();
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class ReplicationStates
//    {
//    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ReplicationState
    {
        public @Nullable String replication_state;
        public @Nullable Double done_percentage;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroup
    {
        public String name = "";
        public String description = "";
        public Map<String, String> props = Collections.emptyMap();
        public @Nullable AutoSelectFilter select_filter;
        /**
         * unique object id
         */
        public @Nullable String uuid;
        public @Nullable Integer peer_slots;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroupModify
    {
        public @Nullable String description;
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
        public @Nullable AutoSelectFilter select_filter;
        public @Nullable Integer peer_slots;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroupSpawn
    {
        /**
         * name of the resulting resource-definition
         */
        public @Nullable String resource_definition_name;
        public @Nullable String resource_definition_external_name;
        /**
         * sizes (in kib) of the resulting volume-definitions
         */
        public List<Long> volume_sizes = Collections.emptyList();
        public @Nullable AutoSelectFilter select_filter;
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
        public @Nullable Integer peer_slots;
        /**
         * For volumes with encryption's, you can provide your own passphrases here.
         */
        public List<String> volume_passphrases = Collections.emptyList();
        public Map<String, String> resource_definition_props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeGroup
    {
        public @Nullable Integer volume_number;
        public Map<String, String> props = Collections.emptyMap();
        /**
         * unique object id
         */
        public @Nullable String uuid;
        public List<String> flags = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class VolumeGroupModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        /**
         * To add a flag just specify the flag name, to remove a flag prepend it with a '-'.
         *
         * Flags:
         *   * GROSS_SIZE
         */
        public List<String> flags = Collections.emptyList();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroupAdjust
    {
        public @Nullable AutoSelectFilter select_filter;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AutoPlaceRequest
    {
        public boolean diskless_on_remaining = false;
        public AutoSelectFilter select_filter = new AutoSelectFilter();
        public @Nullable List<String> layer_list = null;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AutoSelectFilter
    {
        public @Nullable Integer place_count;
        public @Nullable Integer additional_place_count;
        public List<String> node_name_list = Collections.emptyList();
        public @Nullable String storage_pool;
        public @Nullable List<String> storage_pool_list = null;
        public @Nullable List<String> storage_pool_diskless_list = null;
        public @Nullable List<String> not_place_with_rsc = null;
        public @Nullable String not_place_with_rsc_regex;
        public @Nullable List<String> replicas_on_same = null;
        public @Nullable List<String> replicas_on_different = null;
        public @Nullable Map<String, Integer> x_replicas_on_different_map = null;
        public @Nullable List<String> layer_stack = null;
        public @Nullable List<String> provider_list = null;
        public @Nullable Boolean diskless_on_remaining;
        public @Nullable String diskless_type;
        /**
         * Multiplier of thin storage pool's free space
         */
        public @Nullable Double overprovision;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Candidate
    {
        public @Nullable String storage_pool;
        /**
         * maximum size in KiB
         */
        public @Nullable Long max_volume_size_kib;
        public List<String> node_names = Collections.emptyList();
        public @Nullable Boolean all_thin;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class MaxVolumeSizes
    {
        public List<Candidate> candidates = Collections.emptyList();
        public @Nullable Double default_max_oversubscription_ratio;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class QuerySizeInfoRequest
    {
        public @Nullable AutoSelectFilter select_filter;
        public int ignore_cache_older_than_sec = -1;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class QuerySizeInfoResponse
    {
        public @Nullable QuerySizeInfoResponseSpaceInfo space_info;
        public List<ApiCallRc> reports = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class QueryAllSizeInfoRequest
    {
        public @Nullable AutoSelectFilter select_filter;
        public int ignore_cache_older_than_sec = -1;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class QueryAllSizeInfoResponse
    {
        public Map<String, QuerySizeInfoResponse> result = Collections.emptyMap();
        public List<ApiCallRc> reports = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class QuerySizeInfoResponseSpaceInfo
    {
        /**
         * maximum size in KiB
         */
        public long max_vlm_size_in_kib;
        /**
         * available size of the storage pools in KiB
         */
        public @Nullable Long available_size_in_kib;
        /**
         * capacity of the storage pools in KiB
         */
        public @Nullable Long capacity_in_kib;
        public @Nullable Double default_max_oversubscription_ratio;
        public @Nullable Double max_oversubscription_ratio;
        public @Nullable Double max_free_capacity_oversubscription_ratio;
        public @Nullable Double max_total_capacity_oversubscription_ratio;
        public List<QuerySizeInfoSpawnResult> next_spawn_result = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class QuerySizeInfoSpawnResult
    {
        public String node_name;
        public String stor_pool_name;
        public @Nullable Double stor_pool_oversubscription_ratio;
        public @Nullable Double stor_pool_free_capacity_oversubscription_ratio;
        public @Nullable Double stor_pool_total_capacity_oversubscription_ratio;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerPropsModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfig
    {
        public @Nullable ControllerConfigConfig config;
        public @Nullable ControllerConfigDebug debug;
        public @Nullable ControllerConfigLog log;
        public @Nullable ControllerConfigDb db;
        public @Nullable ControllerConfigHttp http;
        public @Nullable ControllerConfigHttps https;
        public @Nullable ControllerConfigLdap ldap;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigConfig
    {
        public @Nullable String dir;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigDebug
    {
        public @Nullable Boolean console_enabled;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigLog
    {
        public @Nullable Boolean print_stack_trace;
        public @Nullable String directory;
        public @Nullable String level;
        public @Nullable String level_global;
        public @Nullable String level_linstor;
        public @Nullable String level_linstor_global;
        public @Nullable String rest_access_log_path;
        public @Nullable String rest_access_mode;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigDb
    {
        public @Nullable String connection_url;
        public @Nullable String ca_certificate;
        public @Nullable String client_certificate;
        public @Nullable String in_memory;
        public @Nullable Boolean version_check_disabled;
        public @Nullable ControllerConfigDbEtcd etcd;
        public @Nullable ControllerConfigDbK8s k8s;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigDbEtcd
    {
        public @Nullable Integer operations_per_transaction;
        public @Nullable String prefix;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigDbK8s
    {
        public @Nullable Integer request_retries;
        public @Nullable Integer max_rollback_entries;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigHttp
    {
        public @Nullable Boolean enabled;
        public @Nullable String listen_address;
        public @Nullable Integer port;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigHttps
    {
        public @Nullable Boolean enabled;
        public @Nullable String listen_address;
        public @Nullable Integer port;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerConfigLdap
    {
        public @Nullable Boolean enabled;
        public @Nullable Boolean public_access_allowed;
        public @Nullable String uri;
        public @Nullable String dn;
        public @Nullable String search_base;
        public @Nullable String search_filter;
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class LogLevel
//    {
//    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ControllerVersion
    {
        public @Nullable String version;
        public @Nullable String git_hash;
        public @Nullable String build_time;
        public @Nullable String rest_api_version;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DatabaseBackupRequest
    {
        public @Nullable String backup_name;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SatelliteConfig
    {
        public @Nullable ControllerConfigConfig config;
        public @Nullable ControllerConfigDebug debug;
        public @Nullable SatelliteConfigLog log;
        public @Nullable String stlt_override_node_name;
        public @Nullable Boolean remote_spdk;
        public @Nullable Boolean ebs;
        public @Nullable Boolean special_satellite;
        public @Nullable String drbd_keep_res_pattern;
        public @Nullable SatelliteConfigNet net;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SatelliteConfigLog
    {
        public @Nullable Boolean print_stack_trace;
        public @Nullable String directory;
        public @Nullable String level;
        public @Nullable String level_linstor;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SatelliteConfigNet
    {
        public @Nullable String bind_address;
        public @Nullable Integer port;
        public @Nullable String com_type;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PropsInfo
    {
        public @Nullable String info;
        public @Nullable String prop_type;
        public @Nullable String value;
        public @Nullable String dflt;
        public @Nullable String unit;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ErrorReport
    {
        public @Nullable String node_name;
        public long error_time;
        /**
         * Filename of the error report on the server.
         *
         * Format is:
         * ```ErrorReport-{instanceid}-{nodeid}-{sequencenumber}.log```
         */
        public @Nullable String filename;
        /**
         * Contains the full text of the error report file.
         */
        public @Nullable String text;
        /**
         * Which module this error occurred.
         */
        public @Nullable String module;
        /**
         * Linstor version this error report was created.
         */
        public @Nullable String version;
        /**
         * Peer client that was involved.
         */
        public @Nullable String peer;
        /**
         * Exception that occurred
         */
        public @Nullable String exception;
        /**
         * Exception message
         */
        public @Nullable String exception_message;
        /**
         * Origin file of the exception
         */
        public @Nullable String origin_file;
        /**
         * Origin method where the exception occurred
         */
        public @Nullable String origin_method;
        /**
         * Origin line number
         */
        public @Nullable Integer origin_line;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ErrorReportDelete
    {
        /**
         * timestamp in millis start date to delete
         */
        public @Nullable Long since;
        /**
         * timestamp in millis for the end date to delete
         */
        public @Nullable Long to;
        /**
         * on which nodes to delete error-reports, if empty/null all nodes
         */
        public List<String> nodes = Collections.emptyList();
        /**
         * delete all error reports with the given exception
         */
        public @Nullable String exception;
        /**
         * delete all error reports from the given version
         */
        public @Nullable String version;
        /**
         * error report ids to delete
         */
        public List<String> ids = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class KeyValueStore
    {
        /**
         * name of the key value store
         */
        public @Nullable String name;
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
    public static class NodeConnection
    {
        /**
         * source node of the connection
         */
        public @Nullable String node_a;
        /**
         * target node of the connection
         */
        public @Nullable String node_b;
        public Map<String, String> props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeConnectionModify
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
        public @Nullable String node_a;
        /**
         * target node of the connection
         */
        public @Nullable String node_b;
        public Map<String, String> props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public @Nullable Integer port;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceConnectionModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class CreateMultiSnapshotRequest
    {
        public List<Snapshot> snapshots = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Snapshot
    {
        public @Nullable String name;
        public @Nullable String resource_name;
        public List<String> nodes = Collections.emptyList();
        @Deprecated(forRemoval = true) public Map<String, String> props = Collections.emptyMap();
        public Map<String, String> snapshot_definition_props = Collections.emptyMap();
        public Map<String, String> resource_definition_props = Collections.emptyMap();
        public List<String> flags = Collections.emptyList();
        public List<SnapshotVolumeDefinition> volume_definitions = Collections.emptyList();
        /**
         * unique object id
         */
        public @Nullable String uuid;
        public List<SnapshotNode> snapshots = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        public List<String> delete_namespaces = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotShippingStatus
    {
        public @Nullable Snapshot snapshot;
        public @Nullable String from_node_name;
        public @Nullable String to_node_name;
        public @Nullable String status;
    }

    /**
     * Objects holding one or multiple SnapshotVolumeNode objects for the given node
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotNode
    {
        /**
         * Snapshot name this snapshots belongs to
         */
        public @Nullable String snapshot_name;
        /**
         * Node name where this snapshot was taken
         */
        public @Nullable String node_name;
        /**
         * milliseconds since unix epoch in UTC
         */
        public @Nullable Long create_timestamp;
        public List<String> flags = Collections.emptyList();
        /**
         * unique object id
         */
        public @Nullable String uuid;
        public Map<String, String> snapshot_props = Collections.emptyMap();
        public Map<String, String> resource_props = Collections.emptyMap();
        public List<SnapshotVolumeNode> snapshot_volumes = Collections.emptyList();
    }

    /**
     * Actual snapshot data from a node
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotVolumeNode
    {
        /**
         * unique object id
         */
        public @Nullable String uuid;
        /**
         * Volume number of the snapshot
         */
        public @Nullable Integer vlm_nr;
        @Deprecated(forRemoval = true) public Map<String, String> props = Collections.emptyMap();
        public Map<String, String> snapshot_volume_props = Collections.emptyMap();
        public Map<String, String> volume_props = Collections.emptyMap();
        /**
         * Optional state for the given snapshot
         */
        public @Nullable String state;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotVolumeDefinition
    {
        public @Nullable Integer volume_number;
        /**
         * Volume size in KiB
         */
        public @Nullable Long size_kib;
        public Map<String, String> snapshot_volume_definition_props = Collections.emptyMap();
        public Map<String, String> volume_definition_props = Collections.emptyMap();
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
        public Map<String, String> stor_pool_rename = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapshotShipping
    {
        /**
         * Node where to ship the snapshot from
         */
        public String from_node;
        /**
         * NetInterface of the source node
         */
        public @Nullable String from_nic;
        /**
         * Node where to ship the snapshot
         */
        public String to_node;
        /**
         * NetInterface of the destination node
         */
        public @Nullable String to_nic;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupList
    {
        /**
         * A list containing all entries found that are or could be from linstor
         */
        public Map<String, Backup> linstor = Collections.emptyMap();
        public @Nullable BackupOther other;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Backup
    {
        public String id;
        public @Nullable String start_time;
        public @Nullable Long start_timestamp;
        public @Nullable String finished_time;
        public @Nullable Long finished_timestamp;
        public String origin_rsc;
        public @Nullable String origin_snap;
        public @Nullable String origin_node;
        public @Nullable String fail_messages;
        public List<BackupVolumes> vlms = Collections.emptyList();
        public @Nullable Boolean success;
        public @Nullable Boolean shipping;
        public @Nullable Boolean restorable;
        public @Nullable BackupS3 s3;
        public @Nullable String based_on_id;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupVolumes
    {
        public long vlm_nr;
        public @Nullable String finished_time;
        public @Nullable Long finished_timestamp;
        public @Nullable BackupVolumesS3 s3;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupVolumesS3
    {
        public @Nullable String key;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupS3
    {
        public @Nullable String meta_name;
    }

    /**
     * A map containing all other entries found that have no relation to linstor
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupOther
    {
        public List<String> files = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupRestore
    {
        public @Nullable String src_rsc_name;
        public @Nullable String src_snap_name;
        public @Nullable String last_backup;
        public Map<String, String> stor_pool_map = Collections.emptyMap();
        public String target_rsc_name;
        public @Nullable String passphrase;
        public String node_name;
        public boolean download_only = false;
        public boolean force_restore = false;
        public @Nullable String dst_rsc_grp;
        /**
         * If the destination resource-definition exists and has resources, the force_mv_rsc_grp must be used in order
         * to change the resource-group of the destination resource-definition. This is a safety-option to prevent
         * unexpected autoplace-actions for example performed by the BalanceResourceTask.
         */
        public boolean force_mv_rsc_grp = false;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupCreate
    {
        public String rsc_name;
        public @Nullable String node_name;
        public @Nullable String snap_name;
        public @Nullable Boolean incremental;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupAbort
    {
        public String rsc_name;
        public @Nullable Boolean restore;
        public @Nullable Boolean create;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupShip
    {
        public @Nullable String src_node_name;
        public String src_rsc_name;
        public String dst_rsc_name;
        public @Nullable String dst_node_name;
        public @Nullable String dst_net_if_name;
        public @Nullable String dst_stor_pool;
        public Map<String, String> stor_pool_rename = Collections.emptyMap();
        public boolean download_only = false;
        public boolean force_restore = false;
        public boolean allow_incremental = true;
        public @Nullable String dst_rsc_grp;
        /**
         * If the destination resource-definition exists and has resources, the force_mv_rsc_grp must be used in order
         * to change the resource-group of the destination resource-definition. This is a safety-option to prevent
         * unexpected autoplace-actions for example performed by the BalanceResourceTask.
         */
        public boolean force_mv_rsc_grp = false;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupInfo
    {
        public String rsc;
        public String full;
        public @Nullable String snap;
        public String latest;
        public @Nullable Integer count;
        public long dl_size_kib;
        public long alloc_size_kib;
        public List<BackupInfoStorPool> storpools = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupInfoRequest
    {
        public @Nullable String src_rsc_name;
        public @Nullable String src_snap_name;
        public @Nullable String last_backup;
        public Map<String, String> stor_pool_map = Collections.emptyMap();
        public @Nullable String node_name;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupInfoStorPool
    {
        public String name;
        public @Nullable String provider_kind;
        public @Nullable String target_name;
        public @Nullable Long remaining_space_kib;
        public List<BackupInfoVolume> vlms = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupInfoVolume
    {
        public @Nullable String name;
        public String layer_type;
        public @Nullable Long dl_size_kib;
        public @Nullable Long alloc_size_kib;
        public @Nullable Long usable_size_kib;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupSchedule
    {
        public @Nullable String rsc_name;
        public @Nullable String grp_name;
        public @Nullable String node_name;
        public @Nullable String dst_stor_pool;
        public Map<String, String> stor_pool_rename = Collections.emptyMap();
        public boolean force_restore = false;
        public @Nullable String dst_rsc_grp;
        /**
         * If the destination resource-definition exists and has resources, the force_mv_rsc_grp must be used in order
         * to change the resource-group of the destination resource-definition. This is a safety-option to prevent
         * unexpected autoplace-actions for example performed by the BalanceResourceTask.
         */
        public boolean force_mv_rsc_grp = false;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ScheduleDetailsList
    {
        public List<ScheduleDetails> data = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ScheduleDetails
    {
        public String remote_name;
        public String schedule_name;
        public @Nullable Boolean ctrl;
        public @Nullable Boolean rsc_grp;
        public @Nullable Boolean rsc_dfn;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ScheduledRscsList
    {
        public List<ScheduledRscs> data = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ScheduledRscs
    {
        public String rsc_name;
        public String remote_name;
        public String schedule_name;
        /**
         * The reason for why this rscDfn has no active schedules.
         * If this is set, ignore all long and boolean parameters.
         */
        public @Nullable String reason;
        /**
         * The time at which the last scheduled shipping was shipped.
         * If negative, no scheduled shipping has happened yet.
         */
        public @Nullable Long last_snap_time;
        /**
         * Whether the last shipping was incremental or not.
         * Ignore this value if last_snap_time is negative.
         */
        public @Nullable Boolean last_snap_inc;
        /**
         * The time at which the next scheduled shipping will happen.
         * If negative, the shipping is currently running.
         */
        public @Nullable Long next_exec_time;
        /**
         * Whether the next scheduled shipping will be incremental or not.
         * Ignore if next_exec_time is negative
         */
        public @Nullable Boolean next_exec_inc;
        /**
         * The time at which the next scheduled full backup should happen.
         * If negative, the time could not be computed
         */
        public @Nullable Long next_planned_full;
        /**
         * The time at which the next scheduled incremental backup should happen.
         * If negative, either there is no cron for incremental backups or
         * the time could not be computed
         */
        public @Nullable Long next_planned_inc;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class BackupQueues
    {
        /**
         * All nodes with a list of queued snapshots. Will be empty
         * if snap_queues is set
         */
        public List<NodeQueue> node_queues = Collections.emptyList();
        /**
         * All snapshots with a list of nodes they are queued on. Will be empty
         * if node_queues is set
         */
        public List<SnapQueue> snap_queues = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeQueue
    {
        public String node_name;
        /**
         * The list of queued snapshots. Will be empty if this is an item of
         * SnapQueue.queue
         */
        public List<SnapQueue> queue = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SnapQueue
    {
        public String resource_name;
        public String snapshot_name;
        public String remote_name;
        public boolean incremental;
        public @Nullable String based_on;
        public long start_timestamp;
        public String pref_node;
        /**
         * The list of nodes this snapshot is queued on. Will be empty if this
         * is an item of NodeQueue.queue
         */
        public List<NodeQueue> queue = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PassphraseStatus
    {
        public String status;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PassPhraseCreate
    {
        public @Nullable String new_passphrase;
        public @Nullable String old_passphrase;
    }

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class PassPhraseEnter
//    {
//    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StoragePoolDefinition
    {
        public @Nullable String storage_pool_name;
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
        public @Nullable Integer port;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdProxyModify
    {
        public Map<String, String> override_props = Collections.emptyMap();
        public List<String> delete_props = Collections.emptyList();
        /**
         * Compression type used by the proxy.
         */
        public @Nullable String compression_type;
        public Map<String, String> compression_props = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorageNode
    {
        public @Nullable Long size;
        public @Nullable Boolean rotational;
        public @Nullable String device;
        public @Nullable String model;
        public @Nullable String serial;
        public @Nullable String wwn;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorage
    {
        public @Nullable Long size;
        public @Nullable Boolean rotational;
        public Map<String, List<PhysicalStorageDevice>> nodes = Collections.emptyMap();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorageDevice
    {
        public @Nullable String device;
        public @Nullable String model;
        public @Nullable String serial;
        public @Nullable String wwn;
    }

    /**
     * This structure is used for create physical-storage
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorageStoragePoolCreate
    {
        /**
         * Name of the linstor storage pool
         */
        public @Nullable String name;
        public Map<String, String> props = Collections.emptyMap();
        /**
         * Name of the shared space
         */
        public @Nullable String shared_space;
        /**
         * true if a shared storage pool uses linstor-external locking, like cLVM
         */
        public boolean external_locking = false;
    }

    /**
     * If `with_storage_pool` is set a linstor storage pool will also be created using this device pool
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class PhysicalStorageCreate
    {
        public String provider_kind;
        public List<String> device_paths = Collections.emptyList();
        /**
         * RAID level to use for pool.
         */
        public String raid_level = "JBOD";
        public @Nullable String pool_name;
        public boolean vdo_enable = false;
        public long vdo_slab_size_kib = 0;
        public long vdo_logical_size_kib = 0;
        public @Nullable PhysicalStorageStoragePoolCreate with_storage_pool;
        /**
         * initialize SED with a random password
         */
        public boolean sed = false;
    }

    /**
     * may-promote-change
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class EventMayPromoteChange
    {
        public @Nullable String resource_name;
        public @Nullable String node_name;
        public @Nullable Boolean may_promote;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class EventNode
    {
        public @Nullable Node node;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class EventNodeModified
    {
        public @Nullable Node old_node;
        public @Nullable Node new_node;
    }

    /**
     * Default settings for EXOS enclosures
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated(forRemoval = true)
    public static class ExosDefaults
    {
        @Deprecated(forRemoval = true) public @Nullable String username;
        @Deprecated(forRemoval = true) public @Nullable String username_env;
        @Deprecated(forRemoval = true) public @Nullable String password;
        @Deprecated(forRemoval = true) public @Nullable String password_env;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated(forRemoval = true)
    public static class ExosDefaultsModify
        extends ExosDefaults
    {
        /**
         * A list of keys to unset. The keys have to exist in ExosDefaults
         */
        public List<String> unset_keys = Collections.emptyList();
    }

    /**
     * EXOS enclosure name, controller IPs and health status
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated(forRemoval = true)
    public static class ExosEnclosureHealth
    {
        @Deprecated(forRemoval = true) public @Nullable String name;
        @Deprecated(forRemoval = true) public @Nullable String ctrl_a_ip;
        @Deprecated(forRemoval = true) public @Nullable String ctrl_b_ip;
        @Deprecated(forRemoval = true) public @Nullable String health;
        @Deprecated(forRemoval = true) public @Nullable String health_reason;
    }

    /**
     * EXOS enclosure
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated(forRemoval = true)
    public static class ExosEnclosure
    {
        @Deprecated(forRemoval = true) public @Nullable String name;
        @Deprecated(forRemoval = true) public @Nullable String ctrl_a_ip;
        @Deprecated(forRemoval = true) public @Nullable String ctrl_b_ip;
        @Deprecated(forRemoval = true) public @Nullable String username;
        @Deprecated(forRemoval = true) public @Nullable String username_env;
        @Deprecated(forRemoval = true) public @Nullable String password;
        @Deprecated(forRemoval = true) public @Nullable String password_env;
    }

    /**
     * EXOS event
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated(forRemoval = true)
    public static class ExosEnclosureEvent
    {
        @Deprecated(forRemoval = true) public @Nullable String severity;
        @Deprecated(forRemoval = true) public @Nullable String event_id;
        @Deprecated(forRemoval = true) public @Nullable String controller;
        @Deprecated(forRemoval = true) public @Nullable String time_stamp;
        @Deprecated(forRemoval = true) public @Nullable Long time_stamp_numeric;
        @Deprecated(forRemoval = true) public @Nullable String message;
        @Deprecated(forRemoval = true) public @Nullable String additional_information;
        @Deprecated(forRemoval = true) public @Nullable String recommended_action;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Deprecated(forRemoval = true)
    public static class ExosConnectionMap
    {
        @Deprecated(forRemoval = true) public @Nullable String node_name;
        @Deprecated(forRemoval = true) public @Nullable String enclosure_name;
        @Deprecated(forRemoval = true) public List<String> connections = Collections.emptyList();
    }

    /**
     * External file which can be configured to be deployed by Linstor
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ExternalFile
    {
        /**
         * The path where the external file should be deployed on the node
         */
        public @Nullable String path;
        /**
         * The contents of the file, in base64 encoding
         */
        public @Nullable String content;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ExtFileCheckResult
    {
        public @Nullable Boolean allowed;
    }

    /**
     * ToggleDisk optional payload data
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ToggleDiskDiskful
    {
        public List<String> layer_list = Collections.emptyList();
    }

//    /**
//     * External name can be used to have native resource names.
//     * If you need to store a non Linstor compatible resource name use this field
//     * and Linstor will generate a compatible name.
//     */
//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    public static class ExternalName
//    {
//    }

    /**
     * Clone request object
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionCloneRequest
    {
        public @Nullable String name;
        public @Nullable String external_name;
        /**
         * If true Zfs will not use send/recv to clone, but instead
         * use a parent snapshot with clone, which cannot be deleted
         */
        public @Nullable Boolean use_zfs_clone;
        public List<String> layer_list = Collections.emptyList();
        /**
         * For volumes with encryption's, you can provide your own passphrases here.
         */
        public List<String> volume_passphrases = Collections.emptyList();
        /**
         * Place clone into the given resource group and use storage pools of this group.
         */
        public @Nullable String resource_group;
    }

    /**
     * Clone request started object
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionCloneStarted
    {
        /**
         * Path for clone status
         */
        public String location;
        /**
         * name of the source resource
         */
        public String source_name;
        /**
         * name of the clone resource
         */
        public String clone_name;
        public List<ApiCallRc> messages = Collections.emptyList();
    }

    /**
     * Clone status object
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionCloneStatus
    {
        /**
         * CLONING -> indicates the resource is currently copying data
         * FAILED -> error occured while cloning, resource not usable
         * COMPLETE -> resource is ready to use
         */
        public String status;
    }

    /**
     * Clone status object
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionSyncStatus
    {
        public boolean synced_on_all = false;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class RemoteList
    {
        public List<S3Remote> s3_remotes = Collections.emptyList();
        public List<LinstorRemote> linstor_remotes = Collections.emptyList();
        public List<EbsRemote> ebs_remotes = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class S3Remote
    {
        public @Nullable String remote_name;
        public @Nullable String endpoint;
        public @Nullable String bucket;
        public @Nullable String region;
        public @Nullable String access_key;
        public @Nullable String secret_key;
        public boolean use_path_style = false;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class LinstorRemote
    {
        public @Nullable String remote_name;
        public @Nullable String url;
        public @Nullable String passphrase;
        public @Nullable String cluster_id;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class EbsRemote
    {
        public @Nullable String remote_name;
        public @Nullable String endpoint;
        public @Nullable String region;
        public @Nullable String availability_zone;
        public @Nullable String access_key;
        public @Nullable String secret_key;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ScheduleList
    {
        public List<Schedule> data = Collections.emptyList();
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class Schedule
    {
        public String schedule_name;
        public String full_cron;
        public @Nullable String inc_cron;
        /**
         * the number of snapshots that are basis for a full backup to keep locally
         */
        public @Nullable Integer keep_local;
        /**
         * the number of full backups to keep at the remote
         */
        public @Nullable Integer keep_remote;
        public String on_failure = "SKIP";
        /**
         * how many times a failed backup should be retried if on_failure == RETRY
         */
        public @Nullable Integer max_retries;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ScheduleModify
    {
        public @Nullable String full_cron;
        public @Nullable String inc_cron;
        /**
         * the number of snapshots that are basis for a full backup to keep locally
         */
        public @Nullable Integer keep_local;
        /**
         * the number of full backups to keep at the remote
         */
        public @Nullable Integer keep_remote;
        public @Nullable String on_failure;
        /**
         * how many times a failed backup should be retried if on_failure == RETRY
         */
        public @Nullable Integer max_retries;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class NodeStats
    {
        public long count;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StoragePoolStats
    {
        public long count;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceGroupStats
    {
        public long count;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceDefinitionStats
    {
        public long count;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ResourceStats
    {
        public long count;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ErrorReportStats
    {
        public long count;
    }

    private JsonGenTypes()
    {
    }
}
