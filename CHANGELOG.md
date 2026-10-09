# Changelog

All notable changes to Linstor will be documented in this file starting from version 1.19.0,
for older version see github releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Satellite: The address of the connected controller is published in `/run/linstor/linstor-client.conf`, which the
  LINSTOR client falls back to instead of localhost if no `linstor-client.conf` was configured
- Controller and satellite now also read all `*.toml` files of `controller.d` / `satellite.d` next to their main
  configuration file, sorted by name, each overriding the values set before; `allowExtFiles` is added to instead
- New paginated error-report list API `GET /v1/view/error-reports`: returns the total count and one page of the
  globally sorted reports; sortable via `sort_by`/`sort_order`, with working `limit`/`offset`, multiple `node`
  filters and an optional `module` filter
- Prometheus: New metric `linstor_node_flag` exports the node flags `DELETE`, `EVICTED` and `EVACUATE` as 0/1 per node
- STORAGE_SPACES: set discard-zeroes-if-aligned to "no" thereby skipping initial resync
- REST: The resource state reports `open` if a DRBD device of the resource is open, also read-only on a Secondary
- Prometheus: New metric `linstor_resource_open` reports whether a DRBD device of the resource is open
- Controller: A controller-only node now reports the platform and OS variant of the host the controller runs on

### Changed

- Satellite: The ErrorReport of a `drbdadm adjust` that fails after a .res file was regenerated now includes the
  regenerated content, which is otherwise lost when the previous file is restored from its backup
- SOS report now includes the LVM devices file `/etc/lvm/devices/system.devices`
- Controller: REST stack upgraded to Jersey 4 (Jakarta REST 4.0), HK2 4 and Guice 7; the REST API itself is unchanged
- Controller: The Kubernetes CRD database backend now uses the JDK HTTP client instead of Vert.x/Netty, removing
  ~6 MB of dependencies (and their CVE exposure) from the distribution
- DRBD: Operations that take a resource down (e.g. delete, deactivate, rollback) are also rejected while its
  device is open on a Secondary, if DRBD reports it

### Fixed

- RPM: Start scripts now use the distribution's JVM directory, fixing startup on SLES/openSUSE 16
- Satellite: Fixed a never-ending storage pool update loop (high CPU, constant DeviceManager cycles) when LVM PVs
  are partitions; block size and discard granularity of partition PVs are now read from their disk
- Resource group create/modify and resource definition modify now reject invalid layer stacks instead of failing
  later when a resource definition is spawned or a resource is created
- Fixed a race between a resource deletion and a concurrent restore or un-delete of the same resource, which made
  the affected satellites shut down (and restart) with a UUID mismatch
- Error report timestamps are no longer shifted by the host's UTC offset on nodes not running in UTC, which also
  made `since`/`to` filters of the error-report listing miss recent reports
- Satellite: Fixed .res file's 'disk' (drbd-peer-options) section not including controller properties when a
  resource-connection exists. Now node-connection and controller properties are also included.
- Fixed rare race condition where after deleting a node the same node could cause "Access to deleted Node"
- Fixed ConcurrentModificationException when serializing SatelliteVolumeState for /metrics and listing resources
  or volumes
- Fixed "Storage pool '...' on node '...' not found" error when satellite tries to update props right after a storage
  pool was deleted. Instead of an ErrorReport only a warning is logged now
- Token authentication: A token's IP filter now matches IPv6 clients in any notation; before, satellite tokens were
  always rejected on IPv6 clusters
- Satellite: Node-connection paths are no longer lost on reconnect if the satellite shares no resource with the
  other node, which made new resources fall back to the default interfaces
- REST: Resource groups now also list their diskless storage pools and node list (`storage_pool_diskless_list`,
  `node_name_list`), which could be set but were never returned
- Fixed rare race condition between BalanceResourcesTask and a user deleting a resource. The result was that a resource
  deleted by the user might have been recreated by the BalanceResourcesTask.
- Controller: Snapshots and clones only flag the topmost layer and WRITECACHE/CACHE layers below it for suspend-IO.
  Since 1.34.0 a LUKS layer below DRBD was suspended too, which with DRBD 9.3 or later stalled snapshots and clones
  until DRBD's ko-count when the Primary replicates to a diskful LUKS replica on another node

## [1.35.2] - 2026-09-14

### Fixed

- Satellite: Clear the read-only flag of a block device before wiping it, so that deleting a volume no longer
  fails with "Failed to wipeFs" if an external user outside of LINSTOR left the device flagged read-only
- Controller: Fixed a database connection leak in the automatic database export, which exhausted the connection
  pool after a number of exports

## [1.35.1] - 2026-09-09

### Added

- Added automatic database exports. Default: every day at 4am. Added configuration properties:
  - "AutoDbExport/Cron": The cron when the auto-export should run. Default: "0 4 * * *". Use "off" to disable feature
  - "AutoDbExport/Keep": How many exported database exports should be kept. Default: 7
  - "AutoDbExport/Path": The path where the exported files should land. Default: "/var/lib/linstor"
  - "AutoDbExport/Compress": True/False whether the export should be compressed or not

### Changed

- "linstor-database export-db" compresses the export if the target file name ends with ".gz"
- "linstor-database import-db" detects compressed exports automatically

### Fixed

- Fixed resizing a volume in a shared storage pool with external locking, refused because of the wrong lock
  mode.
- Fixed the block device info probe racing with udev: the temporary probe volume's device node is now awaited
  and the probe retried, so storage pools no longer silently miss their min/opt I/O size properties.
- Fixed "import-db" failing with "Foreign Key violation" when the export lists a SnapDfn before its RscDfn.
- Fixed "import-db" against non-empty databases.
- Fixed "import-db" reporting "Unknown exception occurred" instead of a useful error message.
- Satellite creating error-reports for parsing null bitmap block-size entries

## [1.35.0] - 2026-08-27

### Added

- ErrorReports now include LINSTOR's cluster-id ("linstor c lp | grep Cluster/LocalID").
- Resource-definition clone: optional `volume_sizes` grows the cloned volumes as part of the clone (before a
  `Clone/BalanceAfterClone` placement)

### Changed

- `DrbdOptions/Resource/*` (e.g. `cpu-mask`) and `DrbdOptions/Handlers/*` properties can now be set on node level
- Deploying an external file with a non-whitelisted path now fails with `FAIL_ACC_DENIED_EXT_FILE` instead of
  `FAIL_UNKNOWN_ERROR`
- Shared storage pools with external locking (e.g. lvmlockd) are now treated like other shared storage pools:
  only one copy is active at a time, and snapshots, cloning and toggle-disk recognize the shared backing data
- make-available with `auto_manage_dual_primary` on such pools now keeps the active copy active for the live
  migration: the backing LVs are locked exclusively, downgraded to shared locks during the dual-active window
- Opening the dual-active window is refused while snapshots (including leftover snapshot data on the shared
  pool) exist or the resource is being cloned; snapshot creation and cloning are refused while dual-active
- Nodes no longer holding the active copy of a shared storage pool now deactivate snapshot LVs and renamed
  (`_deleted_*`) origin LVs, releasing their LV locks and stale device-mapper state

### Fixed

- Fixed the daily log archiving aborting when the log directory contains unexpected entries (e.g. a directory named
  like an error-report); such entries are now skipped, and archived reports are only deleted after tar succeeded
- Fixed race while unsetting "DrbdOptions/ExactSize" which could cause "Resize not allowed during resync" ErrorReports.
- Fixed rare race-condition finding the devicePath in NvmeUtils while deleting a volume-definition.

## [1.35.0-rc.1] - 2026-08-13

### Added

- New `linstor-controller-ha-setup` script that makes the controller highly available via the drbd-reactor promoter,
  optionally with a virtual IP
- Snapshot support for thick LVM (`LVM`) storage pools: create, delete, restore into a new resource and rollback. Each
  snapshot reserves its origin's size as CoW area in the volume group, so it can never become invalid. Restore and
  rollback copy the full data with `dd`. Deleting a resource that still has snapshots renames the backing LV
  (`_deleted_*`); it is removed together with its last snapshot. Volumes with thick LVM snapshots cannot be resized (LVM
  limitation) and backup shipping remains unsupported
- Added a "truncate" operation for resource-definitions that atomically deletes all of a resource-definition's resources
  without touching the resource-definition or its snapshots (`DELETE /v1/resource-definitions/{resource}/resources`); an
  optional flag additionally deletes the resource-definition when it has neither resources nor snapshots left
  afterwards.
- Added an option to snapshot deletion that atomically deletes the resource-definition as well when, after deleting the
  snapshot, the resource-definition has neither resources nor snapshots left.
- make-available: new option `auto_manage_dual_primary` (REST 1.29.0) preparing a resource for a live migration 
  to the given node: for DRBD resources `allow-two-primaries` (and `protocol C` if needed) is set between the migration
  source (the node the resource is in use on) and the target node; for resources in a shared storage pool the resource
  is activated on both nodes at once. If the resource is not in use on any node it is simply made available, so
  clients that cannot distinguish a live-migration attach from a plain attach can always set the option
- New API `unmake-available` reverting a make-available on the migration source after a live migration: removes
  the resource from the node if that is possible without losing data (diskless resources and redundant
  shared-storage-pool copies; tiebreaker and diskful resources are kept) and reverts the DRBD net options set by
  make-available. Calling it for a resource that is not deployed on the node is a successful no-op

### Changed

- Snapshots of resources in shared storage pools are now registered on every node holding a copy of the resource,
  matching the shared data: the snapshot exists once on the shared pool, so every copy effectively holds it. Only
  the node with the active copy performs the snapshot storage operations (create, delete, restore); new or
  (re)activated copies automatically receive the snapshot objects. The active copy can therefore be moved freely
  (make-available, activate) while snapshots exist; if no copy is active at all, one is activated before taking a
  snapshot or rolling back
- Resizing a volume backed by a shared storage pool is now refused while its resource is active on more than one
  node (the dual-active window of a live migration), since the shared data cannot be safely resized while multiple
  nodes are accessing it; conversely, make-available with `auto_manage_dual_primary` refuses to open the
  dual-active window while a volume of the resource is still being resized
- Removed the access-control security subsystem (object protection, ACLs, security identities/roles/types and the
  protobuf sign-in API); it had always been running at `NO_SECURITY` level. The `SEC_*` database tables are dropped on
  upgrade, which makes a downgrade to older versions impossible. LDAP authentication for the REST API, satellite
  connection authentication, auth tokens and HTTPS client certificates are unaffected. Error reports no longer contain
  an access-context section and log lines are no longer prefixed with the security identity
- The controller now refuses to start with a clear error message when the SQL database was already migrated by a newer
  LINSTOR version, instead of failing while partially loading the unknown schema
- Added StltImplErrPeer so that node.getPeer is always non-null
- Upgraded the bundled H2 database from 1.4.197 to 2.4.240. Existing H2 databases are migrated to the new H2 file format
  automatically, either by the package upgrade or on the next start of the linstor-controller service; the migration
  can also be run manually via the new `linstor-database migrate-h2` command.
  The original database file is kept as `<database>.mv.db.h2v1.bak`.
  Old-format `error-report.mv.db` files are moved aside and recreated empty

### Fixed

- Fixed the controller refusing to open an H2 database whose connection URL contains `AUTO_SERVER=TRUE`: H2 2.x
  rejects that setting combined with the internally added `DB_CLOSE_ON_EXIT=FALSE`, which is now skipped instead
- Fixed a DRBD resource deletion that was interrupted after the "prepare deletion" step (e.g. by a satellite
  disconnect or a controller restart) being stuck forever: the resource stayed in the intermediate DRBD_DELETE
  state, leaving an orphaned backing volume on the node and its DRBD peers endlessly trying to connect. The
  deletion is now automatically resumed once all nodes of the resource-definition are connected again
- Fixed snapshot rollback of a resource in a shared storage pool failing when another node holds an inactive copy:
  non-participating inactive copies are no longer recreated after the rollback (they can simply be made available
  again); if no copy is active at all, one is activated before the rollback
- Fixed satellites corrupting the metadata of a shared LVM volume group when starting simultaneously: probing an
  empty thick LVM storage pool for its block-device properties no longer creates a temporary LV (an unserialized
  VG metadata write); the properties are read from the pool's physical volume instead
- Fixed snapshot creation of an IO-suspended volume (e.g. LUKS on a shared storage pool) failing with a `vgscan`
  timeout: the shared-VG rescan no longer opens devices whose IO is suspended
- Fixed make-available for a resource in a shared storage pool failing with "No active resource found" when no
  resource of that shared storage pool was active anywhere (e.g. after cleanly stopping the consumer): the resource is
  now created on the requested node reusing the shared data, ending with a usable (active) resource
- Fixed creating an additional resource that reuses the existing data of a shared storage pool (e.g. via
  make-available) failing the free-space check on a (nearly) full pool: the shared volume already exists and the
  pool's free space already accounts for it, so attaching another resource to it consumes no additional space
- Fixed deleting a thick LVM snapshot re-activating the snapshot (and implicitly its origin LV) before the removal,
  leaving the origin LV of an INACTIVE resource active after the deletion - dangerous for shared storage pools
- Fixed deleting a snapshot in a shared storage pool hanging forever: a device-manager run that only processes snapshots
  (no resources) never requested the shared storage pool locks, so the run aborted with an internal error and the
  deletion was never executed
- Fixed LDAP sign-in using the configured `search_filter` as the LDAP search base; the configured `search_base` was
  never used, so restricting sign-in via a search filter did not work as documented
- Erase LUKS headers with 2 shred passes instead of 4 and only time the shred out when it stops making I/O
  progress, avoiding timeouts on slow storage (e.g. thick-LVM origins with active snapshots); also fixed
  misleading error messages on erase failures
- Fixed deleting a controller property namespace always failing the property whitelist check (the namespace was
  prepended twice to the property keys), rejecting and rolling back the whole modification
- Fixed deleting a resource connection reporting an internal error instead of success (the already deleted connection
  object was accessed when notifying the satellites)
- Fixed modifying a net interface of a node without an active satellite connection failing with an unhandled
  NullPointerException
- Fixed the query-size-info response cache never answering requests whose resource group name was not given in the
  canonical upper-case form, causing needless recomputation
- Fixed several connection API calls reporting an unhandled NullPointerException instead of a proper not-found error:
  creating a resource connection for an unknown resource, modifying a node connection of an unknown node, and
  creating/modifying a volume connection for an unknown volume number or a nonexistent connection
- Another attempt to fix issues with double reconnect
- Fixed possible ConcurrentModificationException during multiple concurrent reconnect attempts
- Fixed incorrect parsing of "" for DISC_GRAN in LsBlkEntry. "" will be parsed as 0
- Fixed AutoHelper now properly concat an updateSatellite at the end of the eagerly subscribed (aka Flux.merge'd)
  additionalFluxList.
- Fixed shared LVM storage pools failing `make-available` with "Logical Volume ... already exists" when another
  node had just created the LV; the cached `lvs`/`vgs` view is now invalidated after the shared-VG `vgscan`

## [1.34.2] - 2026-07-24

### Fixed

- Fixed the `zfs rollback` snapshot rollback strategy reporting success as soon as the satellites confirmed the resource
  re-activation; it now waits for the rolled-back DRBD resources to actually become ready again, like resource creation
  and snapshot restore do
- Fixed a changed DRBD auto verify algorithm not being deployed to already connected satellites when recomputed after a
  node full-sync, leaving nodes with different `verify-alg` settings and forcing their DRBD connections into StandAlone
- Fixed the controller not being able to execute `journalctl` to collect systemd logs by adding it to the
  `systemd-journal` group
- Fixed drbd-proxy disable not releasing the target-side proxy TCP port, leaving it permanently allocated in the
  resource connection and the target node's port pool
- Fixed resource-connection DRBD Proxy ports not being re-reserved in the node TCP port pools on controller startup, so
  a restart could hand out a port still in use by DRBD Proxy
- Fixed the failure path of DRBD port assignment leaving previously reserved ports unreserved in the pool
- Fixed the resource definition staying marked as down on the controller after a successful snapshot rollback using the
  ZFS rollback strategy (the cleared flag was never committed)
- Fixed incorrect usage of SO_REUSEADDR introduced in fb5a9acc04ea40f67494a5d1918d8dfc98d86259. Plus, IP address is no
  longer ignored (i.e. not using 0.0.0.0) when testing ports.

## [1.34.1] - 2026-07-09

### Added

- Controller property `Logging/ArchiveAgeDays` to configure the minimum age in days after which error-report log files
  are archived and compressed (default 60, 0 disables archiving, always whole months); the value is also used by the
  satellites

### Changed

- SOS reports now include DRBD statistics (e.g. out-of-sync): `drbdsetup status` and `drbdsetup events2` are collected
  with `--statistics`, plus a new `drbd-status.json` from `drbdsetup status --json`
- Improved trace logging of DevMgrRuns when creating/merging/deleting core objects
- If resources already violate resource-group's autoplacer settings (like --replicas-on-same), a tiebreaker ignores the
  violated --replicas-on-same limitation. "Place anywhere is better than nowhere".
- Extend ioProgressMode to additionally monitor /sys/[dev/]block/<dev>/stat in addition to the old /proc/<pid>/io to
  survive large flushes.

### Fixed

- Fixed ArrayIndexOutOfBoundsException in the file storage provider when parsing `losetup` output, which broke the
  device manager whenever a loop device was present
- Fixed AutoHTTPS certificate missing SubjectAlternativeName entries
- Fixed AutoHTTPS certificate never being renewed, now valid for 5 years and automatically renews at controller startup
  within 90 days of expiry
- Fixed missing 3370 -> 3371 redirect for the /ui endpoint with AutoHTTPS enabled
- Fixed manual resource creation no longer fails during a setup which already violates resource-group's autoplacer
  settings (like --replicas-on-same)
- Fixed controller package upgrade failing when `/var/lib/linstor` was set immutable (`chattr +i`); the immutable flag
  is now cleared before unpacking

## [1.34.0] - 2026-06-29

### Added

- REST list endpoints now support regular-expression matching in their name filters (`nodes`, `resources`,
  `resource_definitions`, `resource_groups`, `storage_pools`, `snapshots`) and in `props` filters (on both the property
  key and value); the `props` filter is now also available on the per-node storage-pool and per-resource volume list
  endpoints

### Fixed

- Fixed ZFS clone deletion not cleaning up the clone's source snapshot that was marked for deletion, leaving orphaned
  `CF_*` snapshots behind after the cloned resource was deleted
- Fixed DRBD auto verify algorithm being recomputed over an incomplete node set while satellites reconnect, which could
  change the algorithm and force an already connected resource into StandAlone without automatic reconnect
- Fixed implementation error ("attempt to replace an active transMgr") during L2L backup shipping when a new shipment
  created its snapshot or temporary satellite-remote concurrently with the cleanup of a finished shipment deleting its
  temporary satellite-remote
- Fixed implementation error ("attempt to replace an active transMgr") when creating or modifying a resource group
  concurrently with a backup restore/shipment that moves a resource definition into its target resource group
- Fixed BalanceResources counting an unhealthy diskful and possibly deleting therefore a healthy diskful
- Fixed implementation error during DRBD .res file regeneration when the existing on-disk file was empty or truncated,
  which aborted the regeneration instead of rewriting the file
- Fixed regular snapshot restore not setting DRBD_INITIALIZED on the restored volume definition, which could cause
  spurious metadata re-creation and split-brain
- Another attempt to fix rare bug causing "shipping in progress" that can only be cleared by restarting the controller
- Release the target-side restore-lock when a backup receive is aborted (both while still preparing and while actively
  shipping), so a stuck "backup is currently being restored" no longer blocks further shipments to a resource until the
  controller is restarted
- Fixed shipmentFailed always being false in AbsBackupShippingService.waitForSnapCreateFinished
- Fixed NullPointerException on the satellite when concurrently deleting multiple volumes with a `FileSystem/Type`
  property, which marked the resource as failed and returned the delete as an internal error

## [1.34.0-rc.1] - 2026-05-28

### Added

- Toggle-disk can now be retried or aborted.
- Added export-db API
- Satellites report platform and OS variant and Controller exposes that on the REST API.
- Added property "ExtCmd/WaitTimeout"
- Added property "ExtCmd/TermTimeout"
- Added property "ExtCmd/KillTimeout"
- Added property "ExtCmd/IoStallTimeout"
- Added property "ExtCmd/IoPollInterval"
- Added "Linstor/Drbd/OutsideAddress" property for DRBD's outside-address for setups using a NAT
- Added optional alternative suffixes for external files
- Added status check (current path + content as expected) for external files
- Added property "Rest/AutoHTTPs", if enabled will automatically create a SSL certificate and re-start SSL server
- Added token authentication API and implementation, if enabled only allows Authentication Bearer token access
- Added property "Clone/DdBlocksize" to configure the DD clone blocksize used
- Added "BalanceResourcesSkipDiskLimit" property to skip balancing resources with more than X skipDisk resources
  (default 1)
- Added property "Autoplacer/MinThinFreeSpace" that disqualifies storPools with less free space than specified
- Added linstor-config disable-token-auth command
- Property Clone/BalanceAfterClone to autoplace an extra diskful resource after a clone operation
- Property Snapshot/BalanceAfterRestore to autoplace an extra diskful resource after a snapshot restore
- Added property "Luks/AllowDiscards" to enable discard/TRIM pass-through on LUKS volumes; when LUKS sits below DRBD,
  rs-discard-granularity is also set automatically
- Added "Client" as DRBD state (non-voting diskless peer)
- Added property "TcpPortsBlocked" on node level

### Changed

- SOS report now uses export-db instead of non-JSON database dump
- Minimum java version is now java-21, please upgrade if you want to continue building/running LINSTOR.
- DRBD: create-md now only times out if there was no IO for 45s
- mkfs: mkfs.ext4 and mkfs.xfs now only time out if there was no IO for 45s
- Moved old property "ExtCmdWaitTimeout" -> "ExtCmd/WaitTimeout"
- SOS report: 'tar' command now only times out if there was no IO for 45s
- Don't call shred on ZFS devices as those are guaranteed to always read 0
- Upated debian package compat version to 11
- linstor-controller.service: switch to DynamicUser (i.e., execute as non-root user).
- Systemd-notify: Use SDNotify library instead of calling "systemd-notify"
- Thick LVM: New volumes are now placed on the least-used PVs by default. This behavior can be customized via the new
  "StorDriver/LvcreatePvSelection" property.
- Check for valid storage pool names on resource-group select filter
- Controller now ignores case when checking if a NetCom is enabled or not
- Default evict timeout was increased from 1h to 12h
- Property "TcpPortAutoRange" can now also be set on node level

### Removed

- ETCD database support is now removed, please convert to SQL or CRD DB, before upgrade
- Removed K8s migrations older than 2023 / LINSTOR v1.21.0-rc1
- Removed SQL migrations older than 2023 / LINSTOR v1.21.0-rc1

### Fixed

- Fixed incorrect default LUKS2 header size calculation
- Parse errors during ctrl -> stlt authentication now result in "AUTHENTICATION_ERROR" instead of "OFFLINE" (regression
  introduced in 1.32.0-rc.1)
- Fixed free space reporting of thin storage spaces (Windows) pool
- Fixed race condition where after a reconnect storage pools might not show correct capacities and freespaces.
- Fixed PREPARE_SHIPPING backups didn't count as active shippings
- If a satellite cannot decrypt a LUKS key it no longer goes into FULLSYNC_FAILED, but just marks the resource to have
  corrupted crypt key
- Fixed auto-rs-discard-granularity staying set on devices that don't support discards (e.g. LUKS without AllowDiscards)
- Throw a CriticalError(restart) if a JDK spawn helper exception was caught (usually a JDK version mismatch)
- Fixed bug where taking a snapshot on a not-only-DRBD resource could hang indefinitely
- Attempt to fix rare bug causing "shipping in progress" that can only be cleared by restarting the controller
- Fixed rare race condition when changing the LUKS passphrase
- Fixed bug where trying to start a backup shipment of a rsc that isn't UpToDate lead to an AccessToDeletedDataException

## [1.33.3] - 2026-05-11

### Added

- SOS report now includes drbd-reactor config files, logs, and status

### Changed

- AutoUnplacer now prefers non-UpToDate resources after AP-settings-violating resources before choosing any.
- Improved error message for shrink operations
- Disabled DRBD shrink support (It didn't work correctly anyway)
- DRBD: Thin volumes are now set again as UpToDate

### Fixed

- Fixed the clone status check if no DRBD connection states have been received yet
- Fixed volume set size (shrinking) never trigger the actual shrink operation
- Only call "secondary" after an "mkfs" command if LINSTOR was the one that set the DRBD resource primary
- Fixed only reset auto-verify-algo, if at least two nodes reported their crypto entries
- Fixed set-plain-port, set-plain-listen in linstor-config tool using wrong property instances
- Fixed unconditional 2-4s wait time between CRD migrations
- Fixed node properties not properly applied after LINSTOR's FullSync
- Fixed deletion of inactive shared storagepool resources
- Fixed cloning of shared storagepool resource definitions
- Fixed resource create if no .resource object was given
- Prevent NPE if make-available is tried on a resource-definition with no active resources
- Properly reencrypt user-passphrase in VlmDfn during "backup ship"
- Better handle "null" value inputs for json body rest requests with working defaults (nodes/{restore, evacuate), ...)

## [1.33.2] - 2026-04-08

### Changed

- Changed the mechanism how LINSTOR sets DRBD volumes as UptoDate

### Removed

- WinDRBD specific patches regarding missing sha1 and hmac crypto algorithms

### Fixed

- Fixed resource definition delete setting DELETE flag when resources are still in use
- Fixed retry deletion of already deleting resource (no longer fails if it is the second to last diskful resource)
- Satellite now properly merges remote nodes + their data (props, netIfs)
- Fixed potential ConcurrentModificationException when serializing properties
- Fixed race condition in updateController() that leads to uncompleted ChangedController answers (client timeout)
- Fixed bug that could lead to NPE when multiple cache layers are processed in one DevMgr cycle
- Fixed some issues with ReconnectorTask when multiple peers representing the same nodes try to reconnect
- Fixed possible bug that could have caused orphaned DRBD resources being left on the satellite
- Fixed autounplacer not prioritizing autoplacer-settings violations over free space properly
- Fixed EventStreamClosed exception if toggle-disk a single diskful resource
- Fixed that additional volumes now also run mkfs if configured
- Fixed property "Linstor/Drbd/auto-block-size": Now works properly on controller level

## [1.33.1] - 2025-12-22

### Fixed

- Fixed missing transaction-commits during evacuate's copy-snap
- Make sure updateDiscGranularity is not called if only DRBD_DELETE is set
- Also delete error-report text files on error delete api call
- Properly exit on a CriticalError instead of running into a deadlock

## [1.33.0] - 2025-12-11

### Changed

- Added checks if existing resources have enough peerSlots for a new peer

### Fixed

- Clone: Fixed wrong resource-counting for the peerSlots checks
- DrbdProxy: Fixed ignored DrbdProxy/AutoEnable property when set on controller level
- Improved error logging on decryption errors

## [1.33.0-rc.2] - 2025-12-01

### Fixed

- AutoSnapshot: Fixed "Keep" property (regression introduced in 1.32.0)
- Fixed bug where clone no longer works with other min-io size than 512
- Fixed bug where "r c" fails with disabled auto-min-io on a 4k block-sized SP
- Clone: cleanup source snapshot if cloning failed
- Clone: mark failed volumes with DELETE flags to allow to delete them afterward
- Fixed --diskless-on-remaining not working with COMBINED nodes
- Fixed unnecessary restart of local DRBD peer right after the resource was created
- ToggleDisk: Added missing free-space check
- Clone: Added missing free-space check
- Fixed very rare AccessToDeletedNode during a node-update from the satellite
- Fixed possible NPE in DrbdLayer.hasMetaData
- If one resource fails during calculating gross/net size, other resources should still be processed
- Check resource size on creation and resize
- Run autohelper.manage tasks after node fullsync (before only on node rc, c, d)
- Autotiebreaker: do not delete tiebereaker if node is not online/fullsynced
- Cleanup clone task on satellite reconnect
- Clone: Fixed race conditions when starting multiple clones at once

## [1.33.0-rc.1] - 2025-11-11

### Added

- physical-storage-api: allow passing pv/vg/lv/zpool create arguments
- Encryption: Added systemd-creds support for masterpassphrase.
- DRBD: Added Linstor/Drbd/auto-block-size
- Backup: allow shipment of already existing snapshots
- Node,evacuate: Added --target and --do-not-target options

### Changed

- Removed keep_res regular expression pattern from options
- Instead of caching if a DRBD .res file exists, actually check if the file exists
- Do not delete known Linstor drbd res files, LS_KEEP_RES is now obsolete
- Increase max packet size 16M -> 64M
- Increased timeout for tar during sos-report to an hour
- Removed flywaydb dependency and do migrations (as with other "db"s) ourselves
- ReconnectorTask: changed logging levels and added more logging
- API: do not store error reports if required remote parameters are missing
- Node evacuate: By default node evacuate also evacuates existing snapshots

### Fixed

- Attempt to fix race-condition "Received unknown storage resource from satellite"
- Prometheus: fix using peerId as peer address in api call duration statistics (GitHub #466)
- H2 db should no longer prematurely shutdown
- DRBD: recreate ext-metadata after backing disk needed to be re-created
- Fixed a bug where a failed clone leads to AccessToDeletedData bug
- Cache: Properly flush to origin device when suspending
- Fixed toggle-disk bug when evacuation target has already diskless resource
- Fixed possible NPE when a satellite updates a storage pool (capacity, supports snapshots, etc)

## [1.32.3] - 2025-10-13

### Fixed

- Zfs,rollback via restore: Fixed bug where instead of a rollback LINSTOR creates empty ZVOL
- DRBD, rollback: Re-adjust (up) DRBD after rollback is finished (GitHub #464)
- StorPool: Remote storage pools are now properly sent to satellites (not just local)

## [1.32.2] - 2025-10-01

### Changed

- safety-snap: shorten safety-snap id length, to stay in LVM name limit

### Fixed

- Fixed potential deadlock on the controller introduced with 1.32.0
- Clone api: errors are now indicated with HTTP status code 500
- Fixed un-deletable failed clone resources
- Res file regenerate check wasn't working anymore because of added header data

## [1.32.1] - 2025-09-16

### Changed

- S3: Reduce max-parts from 10.000 to 1.000 to better support non-AWS S3 implementations

### Fixed

- node evacuate: if the node had the tiebreaker, tiebreaker creation was accidentally disabled
- ETCD, TcpPort migration: Do not delete old port from LAYER_DRBD_RESOURCE_DEFINITION
- ETCD, TcpPort migration: Fix incorrect nullcheck
- Rsc,list: JSON now contains .layer_object.drbd.connections after reconnect
- Fixed incorrect props_instance for Volume properties within a SnapshotVolume
- DRBD: Fixed still too eager "requires adjust" settings

## [1.32.0] - 2025-09-09

### Changed

- Improved INFO logging on auto-diskful and delete resources
- Try to keep tiebreaker on auto-diskful delete excessive resources

### Fixed

- SQL: SET SCHEMA statement triggered an early transaction start before any data was manipulated
- ScopeRunner: start DB transaction after scope locks are acquired
- mkfs: also use nodiscard for thick volumes, as a fullsync is done anyway
- replication-state-events: ignore events for nodes that are currently unknown
- quickWipe could have been tried with a null device path

## [1.32.0-rc.3] - 2025-08-26

### Added

- Backup,schedule: Add --target-resource-definition. Only usable in combination with "--rd RD"

### Fixed

- Drbd: Fixed bug with introduced drbdadm optimization (resize)
- Backup,restore: Properly set SnapVlmDfn size to avoid issues after rollback/restore

### Removed

- parentIsVDO wasn't working anymore at all, as lsblk didn't report correctly and is not used anymore with LVM-thin on
  VDO

## [1.32.0-rc.2] - 2025-08-18

### Fixed

- Missing volume device path if no DRBD adjust was necessary

## [1.32.0-rc.1] - 2025-08-18

### Added

- Allow setting properties on snapshot definitions
- Added override_props, delete_props and delete_namespaces to the clone resource-definition endpoint
- Added property "StorDriver/Zfs/RollbackStrategy" to allow for old "zfs rollback" behavior
- Added property "StorDriver/Zfs/DeleteStrategy" to fail instead of rename ZVOLs if dependent snapshots exist

### Changed

- LVM-Thin snapshots are set to read-only now
- Cache drbdsetup status output for the device manager run
- DevMgr: Do not conf-test the new resource file, but instead restore the backup on failure
- Ask drbdadm for resources that need to be adjusted instead of adjusting everything all the time
- Snapshots are now automatically deleted during a "node delete" action (only from deleting node, not the peers)
- zfs list: will now mostly only ask for the devices it wants to check instead of a global zfs list
- lsblk output is now parsed with streaming
- parentIsVDO check: only ask for PV member devices
- Improved data types in communication protocol between Controller and Satellite, reducing size by 35%
- **BREAKING**: Unintentionally broke ctrl<->stlt authentication resulting in "OFFLINE" instead of "OFFLINE
  (VERSION_MISMATCH)"

### Fixed

- Fixed possible "attempt to replace active transaction Mgr" when updating free space
- Fixed race condition while cleaning up after cloning for ZFS resources
- Fixed resource definitions not allowing new resource (or rollback/restores) after "backup ship .. --download-only"

### Removed

- EXOS support
- Snapshot shipping (use backup shipping instead!)

## [1.31.3] - 2025-07-10

### Changed

- UUID mismatch will now trigger a System.exit(70) as the best way to recover is to restart the satellite
- API: do not store error reports if user-named node/storage-pool/rg/... was not found
- API: do not store error reports for invalid cron expressions
- Improve output of property changes messages
- Restart Satellite service on exit code 70 (critical error)
- Per default, most linstor operations will now only WARN instead of error if a node is currently not ONLINE

### Fixed

- Fixed error-handling of snapshot-rollback to correctly restore the safety-snap if an error occurs
- Fixed snapshot-rollback ignoring changes to the volume definitions that also need to be reversed
- Fixed vlmDfn UUID mismatch occurring after deleting and later creating a new one with the same vlmNr
- Fixed incorrect zfs-vlm renaming during vlmDfn delete
- Fixed deleting offline node causing "AccessToDeleted node" exception
- Fixed allVolumesUpToDate method while creating a snapshot for diskless resources

## [1.31.2] - 2025-06-11

### Changed

- Snapshot rollback now allows and restores snapshots on nodes that don't have a local snapshot (with DRBD)
- Moved TCP ports from resource definitions to resource level

### Fixed

- Fixed "access to deleted storpool" of remote storage pool after deleting local storage pool
- Fixed AutoplaceTarget property being ignored on controller level
- Fixed AutoplaceTarget property that controller has higher priority than node
- Fixed rare deadlock during shutdown
- Fixed toggle-disk for cache, writecache, bcache, luks and nvme resources
- Fixed order of resourceDefinitionConnected listener notifiers
- Fixed restore/deletion of the safety-snap-* if a snapshot rollback was canceled too early

## [1.31.1] - 2025-05-21

### Changed

- Move generated crd db drivers into own directory to fix gradlew clean
- mkfs ext4 and xfs creation are now forced
- Map replication state entries with actual node names, before drbd peer names were used

### Fixed

- Fixed race condition during "linstor node reconnect" leading to reconnect-cycle
- Fixed duplicate connections to same satellite (older connection gets dropped now)
- Fixed problems with JUnit ProcCryptoUtilsTest
- Fixed possible ConcurrentModificationException when satellite deletes data
- Fixed duplicated minor number bug (based on orphaned .res file)
- Fixed reading wrong property for cache's blocksize
- Fixed incorrect calculation of dm-cache's meta-device size
- Fixed cases where DrbdVlmData's exists boolean was not properly updated
- Fixed rare "access to deleted node" in the PingTask
- Fixed resource-group modify not deleting tiebreaker if quorum property changed
- Fixed auto-quorum/tiebreaker if quorum property was explicitly set to majority

## [1.31.0] - 2025-04-08

### Changed

- Improved TCPConnector logging and only reconnect in client mode
- physical-storage-api: allow VDO with lvm-thin
- ETCD: Enabled DrbdOptions/auto-diskful-allow-cleanup on ctrl unless it was already set
- ReplicationState design wasn't considering replication states per volume connection.
  This has been reworked and now a map is used and returned to the client.

### Fixed

- Weird access to deleted resource bug on drbd down
- CreateNetIf didn't close current peer connection
- UpdateSpaceInfoTask did never commit() its allocation size updates
- Fix deleting of a drbd resource if it was already down

## [1.31.0-rc.2] - 2025-03-21

### Fixed

- Add missing migrations (etcd/crd) for ZFS rename
- Only satellite zfs migrate nodes that have a ZFS pool

## [1.31.0-rc.1] - 2025-03-19

### Added

- Notify controller about replication state changes in DRBD
- Notify controller about done percentage changes in DRBD
- Encryption-API: added a GET endpoint to ask for the master passphrase status

### Changed

- BalanceResource: Ignore resources which are in a non-valid replication state
- sos-report: include backup ship queue
- sos-report: syslog will only be gathered if messages was not available
- Clone: check that a resource name for the cloned resource is set
- Clone: fail clone if the source resource isn't UpToDate or has skip-disk
- Memory: Limit satellite to 2G max memory (-Xmx2G) and controller to 8G
- JVM: will now crash on OutOfMemoryError
- Limit reactor worker threads to 16
- DRBD .res file now contains the time the file was generated
- BalanceResource: Improved selection when deleting excess resource
- AutoDiskfulTask: Improved selection when deleting excess resource
- Tiebreaker: Improved decision making if a deleting resource should be kept as tiebreaker
- Write a full documented linstor.toml and remove the linstor.toml-example
- Snapshot rollback now does internally a resource delete + snapshot restore
- ZFS resource can now be deleted while having ZFS snapshots (ZFS volumes will be renamed)
- ZFS snapshots can now be deleted while having ZFS clones (ZFS snapshots will be renamed)
- Auto-quorum: Reworked auto-quorum property behaviour, property was removed and auto-quorum is now managed through
               the drbd option quorum. on-no-quorum properties will be inherited.
- BackupShip: start next queued shipment immediately when sending fails

### Fixed

- Reconnect: Fixed rare race condition in DRBD version check
- Resource delete: Fixed rare race condition where a delayed DRBD event causes "resource not found" ErrorReports
- Fixed possible UUID mismatch after deletion of last snapshot failed to properly clean up satellite
- Fixed "resource not found" bug when restoring from a snapshot when the source-resource was already deleted
- Satellite: Fixed rare "attempt to replace active transMgr" error
- SATELLITES_CAPACITY table NODE_NAME size was too short
- BackupShip: queued backups are now removed if the target cluster goes offline
- BackupShip: do not use snapshots that are deleting as base for an incremental backup
- BackupShip: remove snapshots from queued backups that have them as base when deleting them

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
