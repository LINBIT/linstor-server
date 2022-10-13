# Changelog

All notable changes to Linstor will be documented in this file starting from version 1.19.0,
for older version see github releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
