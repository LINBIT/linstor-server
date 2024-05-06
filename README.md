[![Open Source](./docs/Linstor-Logo-Colour.png?raw=true)](https://www.linbit.com/linstor)

[![Open Source](https://img.shields.io/badge/Open-Source-brightgreen)](https://opensource.org/) [![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-brightgreen.svg)](https://opensource.org/licenses/) [![Slack Channel](https://img.shields.io/badge/Slack-Channel-brightgreen)](https://join.slack.com/t/linbit-community/shared_invite/enQtOTg0MTEzOTA4ODY0LTFkZGY3ZjgzYjEzZmM2OGVmODJlMWI2MjlhMTg3M2UyOGFiOWMxMmI1MWM4Yjc0YzQzYWU0MjAzNGRmM2M5Y2Q) [![Support](https://img.shields.io/badge/$-support-12a0df.svg?style=flat)](https://www.linbit.com/support/) [![Active](http://img.shields.io/badge/Status-Active-green.svg)](https://linbit.com/linstor) [![GitHub Release](https://img.shields.io/github/release/linbit/linstor-server.svg?style=flat)](https://github.com/LINBIT/linstor-server) [![GitHub Commit](https://img.shields.io/github/commit-activity/y/linbit/linstor-server)](https://github.com/LINBIT/linstor-server) 


# What is LINSTOR®

LINSTOR® developed by LINBIT, is a software that manages replicated volumes across a group of machines. With native integration to Kubernetes, LINSTOR makes building, running, and controlling block storage simple. LINSTOR® is open-source software designed to manage block storage devices for large Linux server clusters. It’s used to provide persistent Linux block storage for cloudnative and hypervisor environments. 

Historically LINSTOR started as a resource-file generator for [DRBD®](https://www.linbit.com/drbd/) which conveniently also created LVM/ZFS volumes. By time LINSTOR steadily grew and got new features and drivers in both directions, south-bound like snapshots, LUKS, dm-cache, dm-writecache or nvme, and north-bound drivers for [Kubernetes](https://www.linbit.com/kubernetes/), [Openstack](https://www.linbit.com/openstack/), [Open Nebula](https://www.linbit.com/opennebula/), [Openshift](https://www.linbit.com/openshift-persistent-container-storage-support/), [VMware](https://www.linbit.com/linstor-vsan-software-defined-storage-for-vmware%e2%80%8b/).


## How it works

LINSTOR system consists of multiple server and client components. A LINSTOR controller manages the configuration of the LINSTOR cluster and all of its managed storage resources. The LINSTOR satellite component manages creation, modification and deletion of storage resources on each node that provides or uses storage resources managed by LINSTOR.

The storage system can be managed by directly using a command line utility to interact with the active LINSTOR controller. Alternatively, users may integrate the LINSTOR system into the storage architecture of other software systems, such as Kubernetes.
All communication between LINSTOR components uses LINSTOR’s own network protocol, based on TCP/IP network connections.

 [![](https://mldatnmifxoe.i.optimole.com/Q4Tiw9A-gKsTX1iL/w:350/h:636/q:auto/https://www.linbit.com/wp-content/uploads/2020/03/How-It-Works2.png)](https://www.linbit.com/linstor/)  [![](https://mldatnmifxoe.i.optimole.com/Q4Tiw9A-22SC98Y2/w:450/h:402/q:auto/https://www.linbit.com/wp-content/uploads/2020/03/unnamed.png)](https://www.linbit.com/linstor/)

## Features
- Open Source


- Main Features
  - Provides replicated block storage and persistent container storage
  - Separation of Data & Control plane
  - Online live migration of backend storage
  - Compatible with high I/O workloads like databases
  - Storage tiering (multiple storage pools)
  - Choose your own Linux filesystem
  - Rich set of [plugins](https://github.com/linbit/linstor-server/blob/master/README.md#plugins)
 

- Storage Related Features
  - Network replication through DRBD integration
  - LVM Snapshot Support
  - LVM Thin Provisioning Support
  - RDMA
  - Management of persistent Memory (PMEM)
  - ZFS support
  - NVME over Fabrics


- Network Related Features
  - Replicate via multiple network cards
  - Automatic management of TCP/IP port range, minor number range etc. provides consistent data
  - Scale-up and scale-out
  - Rest API
  - LDAP Authentification

## User Guide
If you want to use all of the feature set that LINSTOR have (such as quorum, [DRBD](https://www.linbit.com/drbd/) replication etc), you will need at least 3 nodes to use LINSTOR. Linstor-controller and Linstor-client role should be installed on one node and all nodes should have linstor-satellite.

LINSTOR can also perform disk operations without using DRBD. However, if replication with DRBD is desired, DRBD 9 must be installed on all servers. For DRBD installation, please follow [this link](https://www.linbit.com/drbd-user-guide/drbd-guide-9_0-en/).

For a more detailed installation guide, please follow the link below.

[![LINSTOR GUIDE](https://img.shields.io/badge/LINSTOR-GUIDE-orange)](https://www.linbit.com/user-guides/) 

## Plugins

LINSTOR is currently extended with the following plugins. Instructions on how to use them in your own application are linked below.

| Plugin | More Information |
| ------ | ------ |
|iSCSI| https://github.com/LINBIT/linstor-iscsi |
|VSAN|https://www.linbit.com/linstor-vsan-software-defined-storage-for-vmware%e2%80%8b/|
|OpenShift|https://www.linbit.com/openshift-persistent-container-storage-support/|
|OpenNebula|https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-opennebula-linstor|
|Kubernetes|https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-kubernetes|
|OpenStack|https://www.linbit.com/drbd-user-guide/linstor-guide-1_0-en/#ch-openstack-linstor|

## Support

LINSTOR is an open source software. You can use our slack channel above link to get support for individual use and development use.
If you are going to use it in enterprise and mission critical environments, please contact us via the link below for professional support.

[![LINSTOR Support](https://img.shields.io/badge/LINSTOR-SUPPORT-brightgreen)](https://www.linbit.com/support/) 


## Releases
Releases generated by git tags on github are snapshots of the git repository at the given time. They might lack things such as generated man pages, the configure script, and other generated files. If you want to build from a tarball, use the ones [provided by us](https://www.linbit.com/linbit-software-download-page-for-linstor-and-drbd-linux-driver/).

Also for alternative, please look at the "Building" section below. 


## Building
Gradle is used for building LINSTOR. On a fresh git clone some protobuf java files need to be generated and for that a fitting proto compiler is needed. So before building you need to run:
```sh
$ ./gradlew getProtoc
```
After the correct proto compiler is installed in the ./tools directory you can build with:
```sh
$ ./gradlew assemble
```
## Development
Please check the development documentation for details.  

[![LINSTOR Development](https://img.shields.io/badge/LINSTOR-DEVELOPMENT-brightgreen)](https://github.com/LINBIT/linstor-server/blob/master/docs/development.md
) 

## Foundation Membership
[![](https://github.com/LINBIT/linstor-server/blob/master/docs/sodaecop.png)](https://github.com/sodafoundation)

LINSTOR is a SODA ECO Project

**Free Software, Hell Yeah!**

[![LINSTOR Powered by LINBIT](./docs/poweredby_linbit_small.png?raw=true)](https://www.linbit.com/linstor/) 
