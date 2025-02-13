# Changelog

All notable changes to Linstor will be documented in this file starting from version 1.19.0,
for older version see github releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Notify controller about replication state changes in DRBD
- Notify controller about done percentage changes in DRBD
- Encrytion-API: added a GET endpoint to ask for the master passphrase status

### Changed

- BalanceResource: Ignore resources which are in a non-valid replication state
- sos-report: include backup ship queue
- sos-report: syslog will only be gathered if messages was not available
- Clone: check that a resource name for the cloned resource is set
- Clone: fail clone if the source resource isn't UpToDate or has skip-disk
- Memory: Limit satellite to 2G max memory (-Xmx2G) and controller to 8G

### Fixed

- Reconnect: Fixed rare race condition in DRBD version check

## [1.30.4] - 2025-02-03

### Changed

- Changed ext-tool-check command for "drbd-proxy" from "-v" to "--version" to support v3 and v4

### Fixed

- Reconnect: Fixed rare NoSuchFileException triggered by multiple reconnects in quick succession
- BackupShip: Fixed bug where shipment was not executed when previous shipping node was unavailable
- Migration: Fixed incorrect usage of rscGrp's instanceName
- VlmDfn,Resize: Only allow resize if all peers are online and UpToDate

## [1.30.3] - 2025-01-23

### Added

- metrics: Added linstor_node_reconnnect_attempt_count

### Changed

- gradle: support different java home in wrapper scripts
- sos-report: add more controller json(resource-definitions, snapshots) and zfs list snapshots
- /proc/crypto: Ignore invalid entries (i.e. entries with missing driver or type)
- API: do not generate error reports if parent resources are not found
- volume-definition resize: if current size is set, no satellite/data updates will be triggered

### Fixed

- metrics: don't create error-report on closing connection nodes
- LVM: Correctly calculate extents with more than one stripes
- RscDfn and SnapDfn: Properly restore layerStack (!= layerData) from backup and snapshot restore
- BalanceResources: Only adjust selected rscDfn instead of entire rscGrp

## [1.30.2] - 2024-12-18

### Fixed

- Migration: Fixed splitSnapProps bug not properly handling additional properties

## [1.30.1] - 2024-12-17

### Fixed

- Migration: Fixed splitSnapProps bug not properly handling KVS properties

## [1.30.0] - 2024-12-17

### Added

- Added options dst_rsc_grp and force_rsc_grp to BackupShip, BackupRestore and BackupSchedule
- Added timing of REST and internal API calls to prometheus /metrics
- Backup,L2L: Add property to only wait with shipping until snapshot is created

### Changed

- Split snapshot properties into snapshot-specific- and resource-properties
- SOS: use local timestamp as well as relative delta for dmesg output
- SOS: use lvmconfig to show full lvm.conf
- SOS: use lsblk -O, also include json output
- SOS: include /proc/sys/kernel/tainted
- DRBD state tracking: Fixed tracking of a peer's diskless volume's client flag
- Clone: It is now possible to clone into different storage-pools and layer-stacks.
  This can be done by using the new resource-group and layer-stack arguments

### Fixed

- EBS: Fixed retrieving EC2 instance-id

## [1.29.2] - 2024-11-05

### Changed

- luksChangeKey is now also memory limited to 256MB

### Fixed

- Fixed usage of FileSystemWatch to properly trigger when devices appear
- Do not update disc granularity while resource is cloning
- Drbd-layer should not try to adjust resource while still cloning
- Fixed possible "access to deleted resource" in case of an issue during rsc-creation + manual delete
- AutoPlaceAPI: remove unnecessary resource definition loading
- Fixed bug that could be lead to violation of configured --x-replicas-on-different restriction
- Query resource-connection didn't work if source and target node are reversed.
- Fixed unit tests relying on current timezone offset instead of current timezone

## [1.29.1] - 2024-09-24

### Changed

- LVM cache is now time-based, configurable by new property StorDriver/SizesCacheTime
- sedutils-cli calls will now resolve given paths before using them (as sedutils-cli only allows /dev/nvme*)
- resource-definition properties are now included in drbd connection net section
- Attempt to delete LV during DELETE flag even if SkipDisk is enabled (suppressing error reports)
- SOS: Additionally collect journal-based dmesg from multiple previous boots if necessary

### Fixed

- storage-pool list reporting offline nodes even tough everything is connected
- SED setup using same password for all drives
- Fixed stuck deletion of resource during failed / failing resize
- Fixed failing rsc-dfn-update-chain when updating rsc-grp
- Fixed rare race-condition during snapshot create
- Fixed dateformatting with offsets

## [1.29.0] - 2024-07-31

### Changed

- Improved responsiveness: Allow some API calls to be executed concurrently on the satellite
- Added a cache for lvs and vgs/pvs commands, reducing the need to call them
- Changed logback log format to include logid and full timestamp

### Fixed

- Snap, create: Fixed possible deadlock
- Incorrect resource definition already exists error message
- sos-report: Controller not always dumping full log content

## [1.28.0] - 2024-07-11

### Added

- Autoplacer: Add --x-replicas-on-different option
- Resource delete: Add --keep-tiebreaker option

### Changed

- Snapshots now cannot be created while SkipDisk is active
- Autoplacer: Added very small default weight for the MinResourceCount-strategy for better tiebreaker-distribution
- Added some more info logging and improved an error message
- BalanceResourceTask: Do not count skipDisk resources as diskful

### Fixed

- SkipDisk: layers below DRBD now get ignored to prevent errors on the satellites
- ReconnectorTask,EventProcessor: Attempt to fix possible deadlocks while having many connections/requests
- Clone: Fix clone waiting if there is more than one diskless resource
- ToggleDisk: Fix toggle disk for resource spawned with diskles-on-remaining
- Backups: Ensure snapshots can't be stuck in either sending or receiving, which would render them undeletable
- PhassPhraseInit: Fixed uncommitted transaction
- Backup,restore: Check correct SnapVlmDfn property before recalculating AllocationGranularity
- FreeSpace: Fixed incorrect calculation of reserved space for thin volumes

## [1.27.1] - 2024-04-25

### Added

- Support for java-21

### Changed

- Freespace calc: Snapshots only reserve allocated size now instead of volume size
- resource-definition modify: now only warns if a satellite is offline
- AutoVerifyAlgo: allow to use algorithm name additionally to the specific implementation
- ZFS: try to determine default volblocksize

### Fixed

- Deleting a remote can now be safely done even if there are active shippings to said remote
- MakeAvail: trigger update satellite if only FLAGS changed
- Resource delete: Don't check user set verify-algo if last resource deleted.
- If mkfs fails, resources are now being demoted to secondary and can now easily be deleted afterwards
- Fixed incorrect least common multiple call when calculating AllocationGranularity

## [1.27.0] - 2024-04-02

### Added

- Allow users to provide their own encryption passphrase for volumes
- Allow users to provide their own encryption passphrase after cloning
- Allow users to change the passphrase for a luks volume definition
- Backup ship, restore and scheduled backups now have a --force-restore option
- Added more info logging
- Added "DrbdOptions/ExactSize" for migration purposes

### Changed

- Default configuration directory for linstor-database utility (export-db and import-db) is now "/etc/linstor"
- Make-available will no longer return an error if an involved node is offline (warn instead)

### Fixed

- rscDfn-props are no longer reset through --download-only backup restore or if it has rscs deployed
- Fixed snapshot-rollback more than once on a resource
- Fixed re-/ordering issues within the AutoSnapshotTask
- Fixed backup restore not working if one of the backup-parts already existed on the cluster
- Fixed K8s migrations to use correct generated DB table instances
- Deleted invalid SpaceHistory entries from K8s.
- Fixed left-over tiebreaker after evacuation of single DRBD resource
- Fixed negative result of an allocation granularity calculation
- Fixed infinite loop and limited range of power-of-2 calculations
- Workaround for connections where the attached peer object is missing
- Only show warnings of missing cgroup (v1) if the user actually tries to use it
- Fixed undelete resources (also remove volume DELETE and DRBD_DELETE flags)

## [1.26.2] - 2024-02-28

### Changed

- Snapshots and resources are processed independently of each other

### Fixed

- Luks: Properly cleanup LVs when deleting without having entered the passphrase

## [1.26.1] - 2024-02-22

### Changed

- LVM: Removed trailing 'a|.*|' from LVM filter
- Do not write/check drbd resource files if nothing changed
- RG spawn: No longer display "TransactionList" when spawn fails
- Sp,Props: Do not trigger a DevMgrRun for certain property changes
- Tiebreaker: Allow in all even-numbered (>0) diskful and 0 diskless setups
- Improved error message when FullSync fails due to missing external tools
- SOS-report: add more information output (lvs, zfs list, client output....)
- ProblemReports now also get added to the error-database (i.e. will show in up "err list")
- Snapshots: allow taking snapshot if at least one diskfull node is online

### Fixed

- Do not update satellites that are evicted
- Node/Restore: Fixed resource kept in inactive state until controller restart
- Node/Restore: Fixed node not connected on node restore
- Luks: Allow deletion without entered master-passphrase
- SysFs: Warn if /sys/fs/cgroup/blkio does not exist
- When evacuating or migrating an InUse resources, wait until the resource is no longer in-use to remove resource.
- Restoring a pre-SP-mixing-backup into an SP mixing scenario
- SpMixing: ZFS <-> ZFS_THIN is no longer considered as mixed SP
- Encryption: don't create error-reports for user errors

## [1.26.0] - 2024-01-29

## [1.26.0-rc.1] - 2024-01-22

### Added

- ResourceDefinition/VolumeDefinition to prometheus /metrics
- Added PeerSlots to ResourceGroups (create, modify and spawn)
- Added additional oversubscription calculation
- Added new oversubscription ratios to QSI result
- Prometheus/metrics added storage_pool to volume labels
- Added BalanceResourcesTask that in the first version tries to keep the resource-group place-count for resources
- Added MathUtils methods for integer-based square root, exponentiation, prime factorization, LCM and GCD calculation
- Added new checkExtFiles-API
- Schedule enable and snapshot restore now have a --storpool-rename option
- /metrics/scrape-target endpoint with drbd-reactor scrape targets

### Changed

- Added copy of GenDbTables to GenCrdV* classes for migrations
- Escape executed commands in logs, making them paste-able
- Oversubscription now takes minimum of old and of new calculation
- Autoplace API answer now contains object refs to nodes and storagepools
- Make-Available will now retry auto-place with no restrictions on diskless resources
- StoragePoolMixing is now based on extent-sizes and thin/thick. Also changed implementation
- Autoplacer now also allows for mixed storage pools if the corresponding property allows
- Backup ship now uses the --target-storpool as a default storpool name
- ZFS and ZFS_THIN provider now report REFER for thin-volumes and snapshots and VOLSIZE for thick-volumes
- Remember node-ids for a later "forget-peer" when SkipDisk is enabled

### Fixed

- Do not allow setting resource-groups with place-count 0
- Small DB resource leak in health check
- Fixed unrecoverable state with failed rollback of deleting snapshot
- Fixed race condition between node lost and backup queue cleanup
- NetCom SSL reimplementation: Fixed buffer handling, SSL handshake/negotiation/renegotiation
- Prohibit disk accesses in DrbdLayer while SkipDisk is set

### Deprecated

- Support for EXOS

### Removed

- Support for OpenFlex

## [1.25.1] - 2023-11-20

### Fixed

- Fixed version mismatch check between new satellites and a pre v1.25.0 controller
- Fixed "node restore" to try to (re-) create tiebreaker resources if needed
- Fixed "node evacuate" no longer keeps diskless resource as a tiebreaker (in evacuating, but never deleting state)
- Fixed "sp l" showing spurious warning that a diskless SP is offline (due to caching bug)
- Fixed potential null pointer exception in AutoDiskfulTask
- Fixed "r c --drbd-diskless" falsely requiring free peer-slot
- Fixed bug where FILE/-THIN provider did not properly find existing snapshots (thus also not deleting them properly)

## [1.25.0] - 2023-10-25

### Changed

- Added ErrorReport when sending a ping request fails
- Sos-report's tar command no longer use --verbose
- ErrorReports list now works with paging and a default limit.
  Fixing node timeouts on large error-reports
- Resource-group adjust warns now for placement count needs to be reduced (instead of error)

### Fixed

- Fixed setting 'on-no-quorum' to 'suspend-io' at the resource-group level does not propagate down to existing resources
- Fixed logic in setLogLevel to prevent error log if no error happened
- Fixed possible 'Access to deleted Resource' in the AutoDiskfulTask
- Fixed possible left over during AutoDiskfulTask's cleanup of excess resources

## [1.25.0-rc.1] - 2023-10-11

### Added

- Added automated addition of --skip-disk to drbdadm adjust when a disk failure is detected
- Added first version of "effective_props" (currently only used for "DrbdOptions/SkipDisk", other props will follow)
- Added attempt in setting IP_TOS to LOW_DELAY (works in java11 or with forced IPv4)
- Added creation time to ApiCallRc

### Changed

- Improved checks for "OtherController" response
- Do not send fetchFreeSpaces to diskless-only nodes
- Improved responses during tiebreaker takeover

### Fixed

- Fixed possible FileWatch leak
- Fixed toggle-disk now properly setting StorPoolName
- Delete node-connection when last property gets deleted
- Properly delete DRBD_DELETE flag during undelete ('r c' on a DELETING resource)
- Fixed regex-pattern for parsing "dmsetup ls [--target TARGET]" command
- Fixed typo in /v1/action[s]/snapshot/multi
- Fixed incorrectly documented return type of "snapshot create-multi"
- Fixed NPE in schedules etcd driver

## [1.24.2] - 2023-08-30

### Changed

- Improve wrapped DelayedApiException error reporting
- Improve logging on deleting for volumes/snapshots
- Increase timeout for sos create tar command

### Fixed

- Fix "attempt to replace active transMgr" due to unclean core-maps
- Reassigning resource-definition to new resource-group had no effect on satellite
- Use correct java-11 on sles based systems
- Fixed node reconnect scope already entered bug
- Do not create error reports for no connection to satellite errors
- Use timeout 0, to workaround missing setsid -w on old systems
- Backup: Fix possible NPE on non-DRBD resources
- AutoEvict only warn about offline node
- Fix hanging controller on multiple parallel snapshots
- ETCD: fixed exporting empty tables

## [1.24.1] - 2023-08-10

### Fixed

- K8s,Crd: limit number of entries in rollback resource
- gradle: on rhel based systems force to use java-11 jre
- ETCD,import: Convert Date to long

## [1.24.0] - 2023-08-07

### Fixed

- Restore invisible key-value stores, after an old broken migration
- Cleanup orphaned ACL's in database
- Remove java.xml.bind dependency, that didn't work with java-17
- BCache suspend is not supported and wait for device to be created
- Schedules: use correct DB driver for maxretries field
- /metrics: error-report fetching exceptions always handled as timeouts
- RscDfn, peer-slots: fix incorrect cast of peer-slots when modifying resource definitions
- Volume: fix NPE on getApiData if no volumeProvider and no compatStorpool

## [1.24.0-rc.2] - 2023-07-24

### Fixed

- Force all database DATA objects to be comparable

## [1.24.0-rc.1] - 2023-07-20

### Added

- Added support for Microsoft Windows (storagespaces)
- Added queues for backup shipping
- Added linstor-database tool to migrate from/to different database types
- Allow drbd-option properties for node-connections
- Added support for java-17

### Changed

- Update of nearly all 3rd party libraries
- Minimal required java version is now java-11
- Improved suspend-io "downtime" for snapshots 
- Improved default INFO logging level

### Fixed

- Fixed UID-mismatch when creating rollback entries in K8s/CRD
- Resync-After: Update entries after vlm definition delete
- Resync-After: Ignore diskless resources
- Enforce s3-remotes for backup create and linstor-remotes for backup ship
- Allow node-connection drbd options
- Several fixes in backup-shipping
- Storpools can only be deleted without snapshots in addition to without volumes
- Evacuating nodes no longer create snapshots
- Resources can not be created within a resource-definition that is currently restoring a backup

## [1.23.0] - 2023-05-23

### Added

- query-all-size-info API to query-size-info for all currently available resource groups

### Changed

- SOS-Report has now a parent directory in the tgz file
- SharedSPName have now a less strict name checking
- Improved error handling with broken storage pools

### Fixed

- MakeAvailable: Fix incorrect deletion of TIE_BREAKER flag
- SOS-Report missing some rolling .log.zip files
- Offline satellite: Fixed handling of offline satellites
- Rsc,Delete,Takeover: Fix possible double tiebreaker scenarios
- Fixed DRBD config for nodes with purely numerical host names
- Remove/ignore auto-verify-algo property if disabled on controller

## [1.22.1] - 2023-04-27

### Fixed

- NodeCon: Fixed compareTo() causing node connection paths loaded incorrectly.
- LinstorScope not always enter and exit correctly.
- Make-Available API not correctly upgrading a tiebreaker to a diskless resource.

## [1.22.0] - 2023-04-17

### Added

- "StorDriver/LvcreateSnapshotOptions" and "StorDriver/ZfsSnapshotOptions" properties
- "FileSystem/User" and "FileSystem/Group" properties for root-owner of newly created FS
- Support to do multiple snapshots within one device manager run

### Changed

- Autoplacer now ignores occupied values from evicted or evacuating nodes
- DrbdLayer skips processChildren when DRBD_DELETE flag is set

### Fixed

- Backup list can now handle more than 1000 entries in bucket (truncation/continuation was handled wrong before)
- NodeConnection path can now be deleted
- NodeConnections no longer cause NPE on satellites
- Database loading causing ClassCastException with drbd,luks,storage during toggle disk
- Aborted snapshot now always should resume-io

## [1.21.1] - 2023-03-22

### Changed

- auto-resync-after for DRBD will be disabled by default (because it can triggers a DRBD bug)

### Fixed

- Fixed available size reported by query-size-info API

## [1.21.0] - 2023-03-14

### Changed

- Added more details (primary keys) for database-loading exceptions
- ZFS tool checker now uses `zfs -?`

### Fixed

- Fixed apparent ETCD corruption / database-loading bug
- Run resync-after manager on every controller startup

## [1.21.0-rc.1] - 2023-02-28

### Added

- query-size-info API as a better version of query-max-value-size API
- Linstor now automatically sets the resync-after property for drbd resources, grouped by storage pools
- Added automatic rs-discard-granularity management based on lsblk's DISC-GRAN

### Changed

- Added `zfs list` check for ZFS support to ensure that the zfs-utils are also installed, not just the kmod
- Use recommended command sequence for SED initialization and improve error recover

### Fixed

- Fixed DRBD size calculations with non-default peer slots
- Fixed aborting Snapshot procedure (make sure resume-io is executed)
- Fixed AutoSnapshot scheduling bug
- Fixed rollback after clearing entire PropsContainers
- Fixed that too many concurrent Snapshots led to unnecessarily long suspend-io state
- Fixed resource getting activated instead of deactivated in shared-sp

## [1.20.3] - 2023-01-26

### Changed

- pvdisplay now ignores drbd devices(lvm filter)
- Volume definition setSize checks now for out of bounds sizes

### Fixed

- Fixed typo in VDO slab size commandline argument
- Fixed peerslot check also counting diskless resources
- Fixed storage pool definition delete not checking if there are still other storage pools
- Fixed resource definition create showing real exception because of a followup exception
- Fixed resetting resource flags to now incompatible drbd resource flags

## [1.20.2] - 2022-12-14

### Fixed

- Can now use snapshot restore on a snapshot created by a backup create
- Fixed migration of orphan objects

## [1.20.1] - 2022-12-13

### Added

- Added NodeConnection API. Currently only used for path settings (similar to rscCon, but on node level)

### Changed

- Only delete DRBD resource files after successful Controller connection (before on startup)
- MKFS: always skip discard on ZFS backed volumes
- Client resource DRBD ready state as soon as 2 nodes are connected (before it waited for all)
- ResourceGroup-Spawn: Always assume partial mode if resource group doesn't have any volume groups
- Upgraded okhttp 3rd party library, to fix a IPv6 TLS bug
- Satellite now only deletes old .res files after controller established connection (instead of after startup)

### Fixed

- Orphan StorPooDfn, KeyValueStores and SnapDfn are now deleted with all their child objects
- ErrorReports delete ignored since parameter if no other was specified

## [1.20.0] - 2022-10-18

### Added

- Added SnapshotVolumeNode as part of SnapshotNode to include a snapshot state
- Status of EBS volumes and snapshots are now queried periodically
- Added property DrbdOptions/ForceInitialSync that forces thin resources to do an initial full sync

### Fixed

- Snapshots and SnapshotVolumes now properly delete their properties + cleanup migration existing DB entries
- Randomized Cluster/LocalId for CRD setups since all installations had the same ID
- Backup restore now creates new DRBD metadata and forces the initial sync to be a full sync

## [1.20.0-rc.1] - 2022-09-20

### Added

- Added basic support for [SED](https://trustedcomputinggroup.org/resource/self-encrypting-drives-sed-overview/)
- Added support for Amazon EBS storage
- Added ignore reason for storage layers. Used to skip processing resource which would fail anyway.

### Changed

- Improved DB-update-logging
- ZFS: only scan datasets used by Linstor
- Improved auto-diskful mechanic, also removing exceeding replicas

### Fixed

- Fixed backup with mixing internal and external metadata
- Fixed storage-pool size-check on snapshot restore
- Fixed several issues with backup-shipping (NPE, failed shippings, ...)
- Fixed auto snapshot not working after controller restart
- Fixed "LinStor" spelling errors
- Fixed PropsCon concurrentModExc
- Fixed very long k8s CRD DB backend Controller loading time

## [1.19.1] - 2022-07-27

### Fixes

- Fixed loading remotes with ETCD backend
- Autosnapshot: fix property not working on RG or controller

## [1.19.0] - 2022-07-20

### Added

- Added scheduled backup shipping
- SOS-Report support now sending large files
- SOS-Report added more filters

### Changed

- Physical-storage api use LVM VDO commands
- Autoplace: better tiebreaker with autoplace constraints
- API Version 1.14.0

### Fixes

- CRD backend fixes/improvements
- Fix user DRBD verify algorithm list reading/setting
- Attempt to fix stuck volume resizes
- Fix Postgresql external files updates
- Fix support for Google/DigitalOcean S3 remotes
