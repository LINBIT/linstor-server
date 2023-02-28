# Changelog

All notable changes to Linstor will be documented in this file starting from version 1.19.0,
for older version see github releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- query-size-info API as a better version of query-max-value-size API

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
