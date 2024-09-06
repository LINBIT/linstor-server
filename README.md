[![LINSTOR logo](./docs/Linstor-Logo-Colour.png?raw=true)](https://www.linbit.com/linstor)

[![Open Source](https://img.shields.io/badge/Open-Source-brightgreen)](https://opensource.org/) [![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-brightgreen.svg)](https://opensource.org/licenses/) [![Active](http://img.shields.io/badge/Status-Active-green.svg)](https://linbit.com/linstor) [![GitHub Release](https://img.shields.io/github/release/linbit/linstor-server.svg?style=flat)](https://github.com/LINBIT/linstor-server) [![GitHub Commit](https://img.shields.io/github/commit-activity/y/linbit/linstor-server)](https://github.com/LINBIT/linstor-server) [![Enterprise Support](https://img.shields.io/badge/-Enterprise%20Support-f78f22)](https://www.linbit.com/support/) [![Community Forum](https://img.shields.io/badge/-Community%20Forum-1d2a3a)](https://forums.linbit.com/c/linstor/6)

# What is LINSTOR®

LINSTOR&reg;, developed by LINBIT&reg;, is open source software that manages replicated volumes across a group of machines.
LINSTOR natively integrates with Kubernetes and other platforms and makes building, running, and controlling block storage simple.
LINSTOR is designed to manage block storage devices for large Linux server clusters.
It's typically used to provide persistent and highly available Linux block storage for cloud native and hypervisor environments.

Historically LINSTOR started as a [DRBD&reg;](https://www.linbit.com/drbd/) resource file generator which conveniently also created LVM or ZFS volumes.
Over time LINSTOR steadily grew.
LINBIT developers added features and drivers in both directions: south-bound features such as snapshots, LUKS, dm-cache, dm-writecache, or NVMe storage layers; and north-bound drivers for integrating directly with other platforms such as [CloudStack](https://linbit.com/cloudstack/), [Kubernetes](https://www.linbit.com/kubernetes/), [OpenNebula](https://www.linbit.com/opennebula/), [OpenShift](https://www.linbit.com/openshift-persistent-container-storage-support/), [OpenStack](https://www.linbit.com/openstack/), [Proxmox VE](https://www.youtube.com/watch?v=F9xBANiSX0c), and [VMware](https://www.linbit.com/linstor-vsan-software-defined-storage-for-vmware%e2%80%8b/).

## How it works

A LINSTOR system consists of multiple server and client components.
A LINSTOR controller service manages the configuration of the LINSTOR cluster and all of its managed storage resources.
The LINSTOR satellite service manages creation, modification, and deletion of storage resources on each node that provides or uses LINSTOR-managed storage resources.

The storage system can be managed by directly using a command line client utility to interact with the active LINSTOR controller.
Alternatively, users can integrate the LINSTOR system into the storage architecture of other software systems, such as Kubernetes, CloudStack, OpenNebula, or Proxmox VE.
All communication between LINSTOR components uses LINSTOR's own network protocol, based on TCP/IP network connections.

<img src="https://linbit.com/wp-content/uploads/2023/10/How_Linstor_works_enlarged-text.webp" alt="LINSTOR components diagram" width="600" height="536" hspace="10" vspace="10"/>
<img src="https://linbit.com/wp-content/uploads/2023/10/LINSTOR_infographic_2-1024x1024.png" alt="The LINSTOR software stack diagram" height="636" hspace="10" vspace="10"/>

## Features

- Open Source

- Main Features
  - Provides replicated block storage and persistent container storage
  - Separate data and control planes for maximum data availability
  - Online live migration of back-end storage
  - Compatible with high I/O workloads such as databases
  - Supports tiered storage (by using multiple storage pools)
  - Choose your own Linux file system; LINSTOR is agnostic to layers above it
  - Rich set of [plugins](https://github.com/linbit/linstor-server/blob/master/README.md#plugins)

- Storage Related Features
  - Network replication through DRBD integration
  - LVM snapshot support
  - LVM thin provisioning Support
  - RDMA
  - Management of persistent memory (PMEM)
  - ZFS support, including thin provisioning
  - NVMe over fabrics

- Network Related Features
  - Replicate via multiple network cards (for redundancy or load balancing)
  - Automatic management of TCP/IP port range, minor number range, and others to provide consistent data across a cluster
  - Scale-up and scale-out
  - REST API for integrating or customizing to your needs
  - Supports LDAP authentication

## LINSTOR Deployment Architecture Notes

If you want to use the entirety of LINSTOR's feature set (such as quorum, [DRBD&reg;](https://www.linbit.com/drbd/) replication, and others), you will need at least three nodes to use LINSTOR.
A third node, used for quorum purposes, can be diskless and even something as basic as a low-powered single-board computer such as a Raspberry Pi could suffice.
At least one node in the cluster should have the LINSTOR controller service and LINSTOR client software installed, while all nodes should have the LINSTOR satellite service installed.
You can make the LINSTOR controller service highly available by installing the service on multiple nodes.
For more details, refer to the [_LINSTOR User's Guide_](https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#s-linstor_ha).

LINSTOR can also perform disk operations without using DRBD.
However, if you need data replication with DRBD, LINSTOR requires DRBD version 9 installed on LINSTOR satellite nodes.
To install DRBD, refer to the [_DRBD User's
Guide_](https://www.linbit.com/drbd-user-guide/drbd-guide-9_0-en/).

For a more detailed installation guide, refer to instructions in the _LINSTOR User's Guide_, linked below.

[![LINSTOR
GUIDE](https://img.shields.io/badge/LINSTOR-GUIDE-orange)](https://www.linbit.com/user-guides/)

## Plugins

LINSTOR is currently extended with the following plugins.
Instructions for how to use them are linked below.

| Plugin | More Information |
| ------ | ------ |
|CloudStack|https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-cloudstack|
|iSCSI|https://github.com/LINBIT/linstor-iscsi|
|Kubernetes|https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-kubernetes|
|OpenNebula|https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-opennebula-linstor|
|OpenShift|https://www.linbit.com/openshift-persistent-container-storage-support/|
|OpenStack|https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-openstack-linstor|
|Oracle Linux Virtualization Manager|https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-olvm-linstor|
|Proxmox VE|https://linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-proxmox-linstor|
|VSAN|https://www.linbit.com/linstor-vsan-software-defined-storage-for-vmware%e2%80%8b/|

## Support

LINSTOR is an open source software.
For individual or development use, you can use the [LINSTOR forum topic](https://forums.linbit.com/c/linstor/6) within the LINBIT community forums to get support from the greater community of LINBIT software users.
If you are going to use LINSTOR in enterprise or mission critical production environments, professional support contracts and official software binaries are available through LINBIT.
Contact us by using the link below for professional support.

[![LINSTOR Support](https://img.shields.io/badge/LINSTOR-SUPPORT-brightgreen)](https://www.linbit.com/support/)

## Releases

Releases generated by Git tags on GitHub are snapshots of the Git repository at the given time.
These snapshots might lack things such as generated man pages, the configure script, and other generated files.
If you want to build from a tar archive file, use [the ones LINBIT provides](https://www.linbit.com/linbit-software-download-page-for-linstor-and-drbd-linux-driver/).

As an alternative, you can build LINSTOR software from source code.
Refer to the "Building" section below for details.

## Building

Gradle is used for building LINSTOR.
After a fresh `git clone --recursive` command, some protocol buffer ([`protobuf`](https://github.com/protocolbuffers/protobuf)) Java files need to be generated and for that you need a suitable protocol buffer compiler.
Before building you need to run:

```sh
$ ./gradlew getProtoc
```
After installing the correct protocol buffer compiler in the `./tools` directory, you can build LINSTOR by entering the following command:

```sh
$ ./gradlew assemble
```

## Development

Refer to the development documentation for details.

[![LINSTOR Development](https://img.shields.io/badge/LINSTOR-DEVELOPMENT-brightgreen)](https://github.com/LINBIT/linstor-server/blob/master/docs/development.md )

## Foundation Membership
[![](https://github.com/LINBIT/linstor-server/blob/master/docs/sodaecop.png)](https://github.com/sodafoundation)

LINSTOR is a SODA ECO Project

**Free Software, Hell Yeah!**

[![LINSTOR Powered by LINBIT](./docs/poweredby_linbit_small.png?raw=true)](https://www.linbit.com/linstor/)
