# Changelog

All notable changes to Linstor OPENAPI(REST) will be documented in this file.

## [1.26.0]

### Added
  - Add /v1/resource-definitions/{resource}/snapshots/{snapshot} PUT for modifying snapshots
  - Added DrbdResource.tcp_ports
  - Added ResourceCreate.drbd_tcp_port_count
  - Added ResourceCreate.drbd_tcp_ports
  - Added ResourceMakeAvailable.drbd_tcp_ports
  - Added AutoSelectFilter.port_count
  - Added ResourceConnection.drbd_proxy_port_a
  - Added ResourceConnection.drbd_proxy_port_b
  - Added DrbdProxyEnable.port_src
  - Added DrbdProxyEnable.port_target
  - Added ResourceDefinitionCloneRequest.override_props and ResourceDefinitionCloneRequest.delete_props
  - Added components/schemas/SnapshotRollback
  - Added BackupSchedule.dst_rsc_name

### Deprecated
  - ResourceConnection.port

### Removed
  - Removed EXOS:
    - /v1/vendor/seagate/exos/defaults
    - /v1/vendor/seagate/exos/enclosures
    - /v1/vendor/seagate/exos/enclosures/{enclosure}
    - /v1/vendor/seagate/exos/{enclosure}/events
    - /v1/vendor/seagate/exos/map
    - components/schemas/ExosDefaults
    - components/schemas/ExosDefaultsModify
    - components/schemas/ExosEnclosureHealth
    - components/schemas/ExosEnclosure
    - components/schemas/ExosEnclosureEvent
    - components/schemas/ExosConnectionMap
  - Removed deprecated snapshot shipping:
    - /v1/view/snapshot-shippings
    - /v1/resource-definitions/{resource}/snapshot-shipping
    - components/schemas/SnapshotShippingStatus
    - components/schemas/SnapshotShipping

## [1.25.1]

### Fix
  - Add missing query param with_volume_definitions for /v1/resource-definitions GET

## [1.25.0]

### Added
  - Added VolumeState.replication_states
  - Added /v1/encryption/passphrase GET for getting the current encryption status

## [1.24.0]

### Added
  - Added dst_rsc_grp and force_mv_rsc_grp to BackupRestore, BackupShip and BackupSchedule schemas
  - Added snapshot_definition_props and resource_definition_props to Snapshot schema
  - Added snapshot_props and resource_props to SnapshotNode schema
  - Added snapshot_volume_props and volume_props to SnapshotVolumeNode schema
  - Added snapshot_volume_definition_props and volume_definition_props to SnapshotVolumeDefinition schema
  - Added layer_list to resource definition clone request
  - Added resource_group parameter to clone request

### Deprecated
  - Deprecated props of Snapshot schema in favor of snapshot_definition_props and resource_definition_props
  - Deprecated props of SnapshotVolumeNode in favor of snapshot_volume_props and volume_props

## [1.23.0]

### Added
  - Added x_replicas_on_different_map option to AutoSelectFilter schema
  - Added keep_tiebreaker query parameter to DELETE /v1/resource-definitions/{resource}/resources/{node}

## [1.22.0]

### Added
  - Added force_restore parameter to backup ship, restore and schedule backups
  - Added volume_passphrases to resource-group spawn
  - Added volume_passphrases to ResourceDefinitionCloneRequest
  - Added passphrase to volume-definition create
  - Added volume definition PUT /encryption-passphrase endpoint
  - Added initial resource definition properties to spawn command

## [1.21.0]
  - Removed everything OpenFlex related (entry-points, schemas, etc..)
  - Added peerSlots to ResourceGroup (create, modify and spawn)
  - Added stor_pool_free_capacity_oversubscription_ratio and stor_pool_total_capacity_oversubscription_ratio to QuerySizeInfoSpawnResult
  - Added GET /v1/files/{extFileName}/check/{node}
  - Added ExtFileCheckResult
  - Added storpool_rename to SnapshotRestore and BackupSchedule
  - Deprecated EXOS entry points
## [1.20.2]
  - Added EffectivePropertiesMap, EffectivePropertiesMapValue and PropertyWithDescription objects
## [1.20.1]
  - Added max_rollback_entries to ControllerConfigDbK8s
## [1.20.0]
  - Added GET /v1/view/backup/queue
  - Added SnapQueue, NodeQueue, and BackupQueues objects
## [1.19.0]
  - Added POST /v1/queries/resource-groups/query-all-size-info
  - Added QueryAllSizeInfoRequest and QueryAllSizeInfoResponse objects
## [1.18.0]
  - Added POST /v1/action/snapshot/multi as well as CreateMultiSnapshotRequest and CreateMultiSnapshotResponse objects
## [1.17.0]
  - Added POST /v1/resource-groups/{resource_group}/query-size-info
  - Added QuerySizeInfoRequest and QuerySizeInfoResponse objects
## [1.16.0]
  - Added GET /v1/node-connections/  with filter-options ?node_a and ?node_b
  - Added PUT /v1/node-connections/{nodeA}/{nodeB}
  - Added NodeConnection and NodeConnectionModify objects
## [1.15.0]
  - Added GET, POST /v1/remotes/ebs
  - Added PUT /v1/remotes/ebs/{remoteName}
  - Extended RemoteList with ebs_remotes
  - Added POST /v1/nodes/ebs
  - Extended SatelliteConfig with ebs boolean
  - Added SnapshotVolumeNode which includes an optional state
## [1.14.0]
  - Added /v1/physical-storage/{node} to view devices for a single node
  - Added /v1/schedules/ and /v1/schedules/{scheduleName} endpoints as well as Schedule to create, delete, modify and list schedules
  - Added /v1/remotes/{remote_name}/backups/schedule/{schedule_name}/enable, disable and delete to create, delete and modify schedule-remote-rscDfn-triples
  - Added /v1/schedules/list and /v1/schedules/list/{rscName} endpoints as well as ScheduledRscs and ScheduleDetails to list schedule-remote-rscDfn-triples
## [1.13.1]
  - Added k8s options to DB config
  - Added NetCom properties to whitelist
## [1.13.0]
  - Added use_zfs_clone to clone request
  - Added snap_name to backup create
  - Added /v1/events/nodes SSE node events stream
  - Added ?cached=true option for all storage-pool list APIs
  - Added ?limit and ?offset to /v1/nodes/{node}/storage-pools/{storagepool}
  - Added /v1/controller/backup/db to do an database online backup
  - Added /v1/resource-definitions/{resource}/sync-status to check if resource is ready to be used
## [1.12.0]
  - Added /v1/nodes/{node}/evacuate
## [1.11.0]
  - Added /v1/stats/* endpoints for all objects
## [1.10.2]
  - Added /v1/remotes/{remote-name}/backups/info as well as BackupInfo, BackupInfoRequest, BackupInfoStorPool and BackupInfoVolume
  - Added download_only to BackupShip and BackupRestore
## [1.10.1]
  - Added query-param "nodes" for DELETE /v1/resource-definitions/{resource}/snapshots/{snapshot}
  - Added optional Node.eviction_timestamp
  - Added Schema NodeRestore
  - Extended PUT /v1/nodes/{node_name}/restore to accept new NodeRestore body
## [1.10.0]
  - Added resource-definition clone API
    - POST /v1/resource-definitions/{rscname}/clone
    - GET /v1/resource-definitions/{rscname}/clone/{clonedrsc}
  - Added /v1/resource-groups/{resource_group}/adjust
  - Added /v1/resource-groups/adjustall
  - Added Backup and BackupList, as well as the entrypoint /v1/remotes/{remote_name}/backups
  - Added S3Remote and the corresponding entrypoint /v1/remotes
  - Added LinstorRemote and the corresponding entrypoint /v1/remotes
## [1.9.0]
  - Extended SatelliteConfig with remote_spdk boolean
## [1.8.1]
  - Added layer_stack to toggle-disk API
## [1.8.0]
  - Added layer BCACHE
    - Schemas BCacheResource and BCacheVolume
## [1.7.0]
  - Added shared_name to ResourceWithVolumes
  - Added makeResourceAvailable API
  - Added diskless storage pool filter to AutoSelectFilter
  - Added external_locking to StoragePool
  - Added shared_space and external_locking to PhysicalStorageStoragePoolCreate
  - Added shared_space to StoragePool (for listing)
  - Added EXOS API
    - /v1/vendor/seagate/exos/defaults
    - /v1/vendor/seagate/exos/enclosure/
    - /v1/vendor/seagate/exos/enclosure/{enclosure}
    - /v1/vendor/seagate/exos/enclosure/{enclosure}/events
    - /v1/vendor/seagate/exos/map
  - Added ExternalFiles API with corresponding data structure
    - /v1/files
    - /v1/files/{extFileName}
    - /v1/resource-definitions/{resource}/files/{extFileName}
  - Added de-/activate to resource API
    - /v1/resource-definitions/{resource}/resources/{node_name}/activate
    - /v1/resource-definitions/{resource}/resources/{node_name}/deactivate
## [1.6.0]
  - Added the PropsInfo API which exposes meta information about properties:
    - /v1/controller/properties/info
    - /v1/controller/properties/info/all
    - /v1/nodes/properties/info
    - /v1/storage-pool-definitions/properties/info
    - /v1/nodes/{node}/storage-pools/properties/info
    - /v1/resource-definitions/properties/info
    - /v1/resource-definitions/{resource}/resources/properties/info
    - /v1/resource-definitions/{resource}/volume-definitions/properties/info
    - /v1/resource-definitions/{resource}/resources/{node}/volumes/properties/info
    - /v1/resource-definitions/{resource}/resource-connections/properties/info
    - /v1/resource-groups/properties/info
    - /v1/resource-groups/{resource_group}/volume-groups/properties/info
    - /v1/resource-definitions/{resource}/drbd-proxy/properties/info
  - Added /v1/nodes/{node}/restore
  - Added additional_place_count to AutoSelectFilter
## [1.5.0]
  - Added etcd.prefix to ControllerConfigDbEtcd parameters
## [1.4.0]
  - Added promotion_score and may_promote to DrbdResource object
  - Added /v1/error-reports DELETE method, to delete a range of error reports or single ones
  - Added SSE (Server Sent Events) url /v1/events/drbd/promotion
## [1.3.0]
  - Added /v1/view/snapshot-shippings
## [1.2.0]
  - Added optional AutoSelectFilter to resource-group/spawn
  - Added /v1/nodes/{node}/config, that allows you to get and set the satellite config
  - Added /v1/sos-report to create bug reports you can send to linbit
  - Added new fields to the ErrorReport object
  - Added /v1/resource-definitions/{resource}/snapshot-shipping
  - Allow to modify the resource group in Resource definitions
  - Added createTimestamp to Resource and Snapshot
  - Added default value (null) for AutoPlaceRequest's layer_list
## [1.1.0]
  - Added /v1/view/snapshots for a faster all in one snapshot list
  - Filter lists by properties:
    - /v1/nodes
    - /v1/resource-definitions
    - /v1/resource-groups
    - /v1/view/storage-pools
    - /v1/view/resources
## [1.0.16]
  - Added CacheResource and CacheVolume schemas
  - AutSelectFilter arrays are now null per default
## [1.0.15]
  - Added connections map to the DRBD resource layer data
  - Added support for Openflex
## [1.0.14]
  - Added /v1/controller/config, that gives you the controller config information
## [1.0.13]
  - Fixed broken volume definition modify `flags` handling
  - Added flags to volume groups (create/modify)
## [1.0.12]
  - Added WritecacheResource and WritecacheVolume schemas.
  - Removed support for swordfish
  - Added `with_storage_pool` to PhysicalStorageCreate post request, allowing to create linstor storage pools too
  - Added `gross` flag for volume-definition size
  - Added flags to VolumeDefinitionModify (so that `gross` flag can be changed)
  - Added query-max-volume-size to resource-groups
## [1.0.11]
  - Added /v1/physical-storage endpoint, that lets you query and create lvm/zfs pools
  - Extended Node with list of supported providers and layers as well as lists of reasons for
      unsupported providers and layers
## [1.0.10]
  - Added `reports` array field to Volume object, contains ApiCallRcs for problems
  - Changed `ResourceDefinitions` can now include `VolumeDefinitions` in `volume_definitions` field
  - Added various filter query parameters
## [1.0.9]
  - Added supports_snapshots to StoragePool
## [1.0.8]
  - Added /v1/resource-groups
  - Added /v1/resource-groups/{rscgrp}/volume-groups
  - Moved AutoSelectFilter::place_count default indirectly to create resource implementation
  - Added diskless_on_remaining to AutoSelectFilter
  - Changed /v1/view/resources return type to ResourceWithVolumes
      ResourceWithVolumes is now a child type of Resource (removed volumes from Resource)
## [1.0.7]
  - Added ext_meta_stor_pool to DrbdVolume
  - Added is_active field to the NetInterface type
## [1.0.6]
  - Added /v1/resource-definitions/{rscName}/resources/{nodeName}/volumes/{vlmnr} PUT
## [1.0.5]
  - Added `reports` field to StoragePool object
## [1.0.4]
  - Added /v1/view/storage-pools overview path
  - Added uuid fields for objects
## [1.0.3]
  - Added /v1/view/resources overview path
  - documentation schema extraction
## [1.0.2]
  - Added /v1/storage-pool-definitions object path
  - added NVME layer object type
## [1.0.1]
  - Documentation review and updates
  - no functional changes
## [1.0.0]
  - Initial REST API v1
