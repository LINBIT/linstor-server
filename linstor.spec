Name: linstor
Version: 1.6.1
Release: 1%{?dist}
Summary: LINSTOR SDS
BuildArch: noarch
%define GRADLE_TASKS installdist
%define GRADLE_FLAGS --offline --gradle-user-home /tmp --no-daemon --exclude-task generateJava
%define LS_PREFIX /usr/share/linstor-server
%define FIREWALLD_SERVICES /usr/lib/firewalld/services
%define NAME_VERS %{name}-server-%{version}

# Prevent brp-java-repack-jars from being run.
%define __jar_repack %{nil}

Group: System Environment/Daemons
License: GPLv2+
URL: https://github.com/LINBIT/linstor-server
Source0: http://www.linbit.com/downloads/linstor/linstor-server-%{version}.tar.gz

%if 0%{?suse_version} >= 1500
BuildRequires: java-1_8_0-openjdk-headless java-1_8_0-openjdk-devel python
%else  # rhel
BuildRequires: java-1.8.0-openjdk-headless java-1.8.0-openjdk-devel python2
%endif

%description
TODO.


%prep
%setup -q -n %{NAME_VERS}


%build
rm -rf ./build/install
gradle %{GRADLE_TASKS} %{GRADLE_FLAGS}
for p in server satellite controller; do echo "%{LS_PREFIX}/.$p" >> "%{_builddir}/%{NAME_VERS}/$p/jar.deps"; done

%install
mkdir -p %{buildroot}/%{LS_PREFIX}
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/lib %{buildroot}/%{LS_PREFIX}
rm %{buildroot}/%{LS_PREFIX}/lib/%{NAME_VERS}.jar
cp -r %{_builddir}/%{NAME_VERS}/server/build/install/server/lib/conf %{buildroot}/%{LS_PREFIX}/lib
mkdir -p %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/bin/Controller %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/bin/Satellite %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/bin/linstor-config %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/scripts/postinstall.sh %{buildroot}/%{LS_PREFIX}/bin/controller.postinst.sh
mkdir -p %{buildroot}/%{_unitdir}
cp -r %{_builddir}/%{NAME_VERS}/scripts/linstor-controller.service %{buildroot}/%{_unitdir}
cp -r %{_builddir}/%{NAME_VERS}/scripts/linstor-satellite.service %{buildroot}/%{_unitdir}
mkdir -p %{buildroot}/%{FIREWALLD_SERVICES}
cp %{_builddir}/%{NAME_VERS}/scripts/firewalld/drbd.xml %{buildroot}/%{FIREWALLD_SERVICES}
cp %{_builddir}/%{NAME_VERS}/scripts/firewalld/linstor-controller.xml %{buildroot}/%{FIREWALLD_SERVICES}
cp %{_builddir}/%{NAME_VERS}/scripts/firewalld/linstor-satellite.xml %{buildroot}/%{FIREWALLD_SERVICES}
mkdir -p %{buildroot}/%{_sysconfdir}/drbd.d/
cp %{_builddir}/%{NAME_VERS}/scripts/linstor-resources.res %{buildroot}/%{_sysconfdir}/drbd.d/
touch %{buildroot}/%{LS_PREFIX}/{.server,.satellite,.controller}
mkdir -p %{buildroot}/%{_sysconfdir}/linstor
cp %{_builddir}/%{NAME_VERS}/docs/linstor.toml-example %{buildroot}/%{_sysconfdir}/linstor/

### common
%package common
Summary: Common files shared between controller and satellite
Requires: jre-headless

%description common
Linstor shared components between linstor-controller and linstor-satellite


%files common -f %{_builddir}/%{NAME_VERS}/server/jar.deps
%dir %{LS_PREFIX}
%dir %{LS_PREFIX}/lib
%{LS_PREFIX}/lib/server-%{version}.jar
%dir %{LS_PREFIX}/lib/conf
%{LS_PREFIX}/lib/conf/logback.xml

### controller
%package controller
Summary: Linstor controller specific files
Requires: linstor-common = %{version}

%description controller
Linstor controller manages linstor satellites and persistant data storage.


%files controller -f %{_builddir}/%{NAME_VERS}/controller/jar.deps
%dir %{LS_PREFIX}
%dir %{LS_PREFIX}/lib
%{LS_PREFIX}/lib/controller-%{version}.jar
%dir %{LS_PREFIX}/bin
%{LS_PREFIX}/bin/Controller
%{LS_PREFIX}/bin/linstor-config
%{LS_PREFIX}/bin/controller.postinst.sh
%{_unitdir}/linstor-controller.service
%{FIREWALLD_SERVICES}/linstor-controller.xml
%{_sysconfdir}/linstor/linstor.toml-example

%post controller
%{LS_PREFIX}/bin/controller.postinst.sh
%systemd_post linstor-controller.service
test -f %{_bindir}/firewall-cmd && firewall-cmd --reload --quiet || :

%preun controller
%systemd_preun linstor-controller.service

### satellite
%package satellite
Summary: Linstor satellite specific files
Requires: linstor-common = %{version}
Requires: lvm2

%description satellite
Linstor satellite, communicates with linstor-controller
and creates drbd resource files.


%files satellite -f %{_builddir}/%{NAME_VERS}/satellite/jar.deps
%dir %{LS_PREFIX}
%dir %{LS_PREFIX}/lib
%{LS_PREFIX}/lib/satellite-%{version}.jar
%dir %{LS_PREFIX}/bin
%{LS_PREFIX}/bin/Satellite
%{_unitdir}/linstor-satellite.service
%{FIREWALLD_SERVICES}/linstor-satellite.xml
%{FIREWALLD_SERVICES}/drbd.xml
%config(noreplace) %{_sysconfdir}/drbd.d/linstor-resources.res

%post satellite
%systemd_post linstor-satellite.service
test -f %{_bindir}/firewall-cmd && firewall-cmd --reload --quiet || :

%preun satellite
%systemd_preun linstor-satellite.service

%changelog
* Tue Apr 14 2020 Gabor Hernadi <gabor.hernadi@linbit.com> 1.6.1-1
- New upstream release. Fix startup bug.

* Wed Apr 08 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.6.0-1
- New upstream release. Cache layer, resource group modify fixes, bug fixes.

* Tue Mar 24 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.5.2-1
- New upstream release. Fix LVM-filters on physical-storage.

* Mon Mar 23 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.5.1-1
- New upstream release. Add LVM-filters, fix db password login.

* Wed Mar 11 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.5.0-1
- New upstream release. OpenFlex support, drbd-connection info, bug fixes.

* Tue Mar 3 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.4.3-1
- New upstream release. Remove ETCD trans-limit, bug fixes.

* Mon Jan 27 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.4.2-1
- New upstream release. VlmGrp gross-size, resize bug-fix.

* Thu Jan 16 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.4.1-1
- New upstream release. DB migration fixes.

* Tue Jan 14 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.4.0-1
- New upstream release. Write-cache, Snapshot-fixes, bug fixes.

* Thu Dec 5 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.3.1-1
- New upstream release. ETCD-migrations, REST-API fixes, bug fixes.

* Tue Dec 3 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.3.0-1
- New upstream release. SPDK support, bug fixes.

* Thu Nov 7 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.2.1-1
- New upstream release. ETCD-ssl support, bug fixes.

* Thu Oct 24 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.2.0-1
- New upstream release. Auto-tiebreaker, bug fixes.

* Mon Sep 16 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.1.2-1
- New upstream release. Fix resource connection loading.

* Tue Sep 10 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.1.1-1
- New upstream release. Bug fixes.

* Fri Aug 30 2019 Gabor Hernadi <gabor.hernadi@linbit.com> 1.1.0-1
- New upstream release. Added ETCD support.

* Fri Aug 9 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.0.1-1
- New upstream release. Fix postgresql migration.

* Thu Aug 8 2019 Rene Peinthor <rene.peinthor@linbit.com> 1.0.0-1
- New upstream release. Added resource groups, REST-API 1.0.8, Bug fixes.

* Thu Jul 25 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.13-1
- New upstream release. File provider, Toml config, REST-API 1.0.7, Bug fixes.

* Tue Jun 25 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.12-1
- New upstream release. REST-API 1.0.6, Bug fixes.

* Tue Jun 11 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.11-1
- New upstream release. REST-API 1.0.5, Bug fixes.

* Mon May 27 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.10-1
- New upstream release. Bug fixes.

* Thu May 23 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.9-1
- New upstream release. REST-API 1.0.4, bug fixes.

* Thu May 16 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.8-1
- New upstream release. NVMe fixes, REST-API 1.0.3, bug fixes.

* Thu May 9 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.7-1
- New upstream release. Bug fixes.

* Thu Apr 18 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.6-1
- New upstream release. NVMe storage layer, bug fixes.

* Mon Apr 8 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.5-1
- New upstream release. Migration multi volume fixes.

* Fri Mar 29 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.4-1
- New upstream release. Fix zfs root thin pools and zfs dataset pools.

* Thu Mar 28 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.3-1
- New upstream release. Fix zfs storage pools and activate lvm-thin volumes.

* Mon Mar 25 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.2-1
- New upstream release. Fix persisting layer stacks.

* Mon Mar 25 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.1-1
- New upstream release. REST-API, DeviceManager Rework, bug fixes.

* Fri Mar 15 2019 Rene Peinthor <rene.peinthor@linbit.com> 0.9.0-1
- New upstream release. REST-API, DeviceManager Rework.

* Fri Dec 21 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.7.5-1
- New upstream release. Bug fixes.

* Mon Dec 17 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.7.4-1
- New upstream release. Bug fixes.

* Thu Nov 22 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.7.3-1
- New upstream release. Bug fixes.

* Mon Nov 12 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.7.2-1
- New upstream release. Bug fixes.

* Tue Nov 06 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.7.1-2
- Correctly clean intermediate build files.

* Wed Oct 31 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.7.1-1
- New upstream release. Fix thin resource deletion.

* Tue Oct 30 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.7.0-1
- New upstream release. Proxy support, transactional resource create, ...

* Tue Oct 02 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.6.5-1
- New upstream release. Sync delete, fixes.

* Fri Sep 14 2018 Rene Peinthor <rene.peinthor@linbit.com> 0.6.4-1
- Atomic move fix, fix storage pool list on error case

* Mon Jul 30 2018 Roland Kammerer <roland.kammerer@linbit.com> 0.2.6-1
- New upstream release.
