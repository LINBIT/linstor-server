Name: linstor
Version: 1.30.3
Release: 1%{?dist}
Summary: LINSTOR SDS
BuildArch: noarch
%define GRADLE_TASKS installdist
%define GRADLE_FLAGS --offline --gradle-user-home /tmp --no-daemon --exclude-task generateJava
%define LS_PREFIX /usr/share/linstor-server
%define FIREWALLD_SERVICES /usr/lib/firewalld/services
%define FILE_VERSION %(echo %{version} | sed -e 's/~/\-/')
%define NAME_VERS %{name}-server-%{FILE_VERSION}

# Prevent brp-java-repack-jars from being run.
%define __jar_repack %{nil}

Group: System Environment/Daemons
License: GPLv2+
URL: https://github.com/LINBIT/linstor-server
Source0: http://pkg.linbit.com/downloads/linstor/linstor-server-%{FILE_VERSION}.tar.gz

%if 0%{?suse_version} >= 1500
BuildRequires: java-11-openjdk-headless java-11-openjdk-devel python
%define GRADLE_JAVA_HOME -PjavaHome=/usr/lib64/jvm/jre-11
%else
%if 0%{?rhel} > 9
BuildRequires: java-21-openjdk-headless java-21-openjdk-devel python3
%define GRADLE_JAVA_HOME -PjavaHome=/usr/lib/jvm/jre-21
%else
%if 0%{?rhel} > 7
BuildRequires: java-11-openjdk-headless java-11-openjdk-devel python3
%define GRADLE_JAVA_HOME -PjavaHome=/usr/lib/jvm/jre-11
%else
BuildRequires: java-11-openjdk-headless java-11-openjdk-devel python2
%define GRADLE_JAVA_HOME -PjavaHome=/usr/lib/jvm/jre-11
%endif
%endif
%endif

%description
TODO.


%prep
%setup -q -n %{NAME_VERS}


%build
rm -rf ./build/install
gradle %{GRADLE_TASKS} %{GRADLE_FLAGS} %{?GRADLE_JAVA_HOME}
for p in server satellite controller jclcrypto; do echo "%{LS_PREFIX}/.$p" >> "%{_builddir}/%{NAME_VERS}/$p/jar.deps"; done

%install
mkdir -p %{buildroot}/%{LS_PREFIX}
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/lib %{buildroot}/%{LS_PREFIX}
if [ -f "%{_builddir}/%{NAME_VERS}/libs/server-st.jar" ]; then cp "%{_builddir}/%{NAME_VERS}/libs/server-st.jar" %{buildroot}/%{LS_PREFIX}/lib; fi
if [ -f "%{_builddir}/%{NAME_VERS}/libs/controller-st.jar" ]; then cp "%{_builddir}/%{NAME_VERS}/libs/controller-st.jar" %{buildroot}/%{LS_PREFIX}/lib; fi
if [ -f "%{_builddir}/%{NAME_VERS}/libs/satellite-st.jar" ]; then cp "%{_builddir}/%{NAME_VERS}/libs/satellite-st.jar" %{buildroot}/%{LS_PREFIX}/lib; fi
rm %{buildroot}/%{LS_PREFIX}/lib/%{NAME_VERS}.jar
mkdir -p %{buildroot}/%{LS_PREFIX}/lib/conf
cp %{_builddir}/%{NAME_VERS}/server/logback.xml %{buildroot}/%{LS_PREFIX}/lib/conf
mkdir -p %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/bin/Controller %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/bin/Satellite %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/bin/linstor-config %{buildroot}/%{LS_PREFIX}/bin
cp -r %{_builddir}/%{NAME_VERS}/build/install/linstor-server/bin/linstor-database %{buildroot}/%{LS_PREFIX}/bin
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
%if 0%{?rhel} > 9
Requires: java-21-openjdk-headless
%else
Requires: jre-11-headless
%endif
## This should really be included in the jre-headless dependencies, but it isn't.
Requires: tzdata-java

%description common
Linstor shared components between linstor-controller and linstor-satellite


%files common -f %{_builddir}/%{NAME_VERS}/server/jar.deps
%dir %{LS_PREFIX}
%dir %{LS_PREFIX}/lib
%{LS_PREFIX}/lib/server-%{FILE_VERSION}.jar
%{LS_PREFIX}/lib/jclcrypto-%{FILE_VERSION}.jar
%dir %{LS_PREFIX}/lib/conf
%{LS_PREFIX}/lib/conf/logback.xml

### controller
%package controller
Summary: Linstor controller specific files
Requires: linstor-common = %{version}
%if 0%{?rhel} > 9
Requires(post): java-21-openjdk-headless
%else
Requires(post): jre-11-headless
%endif

%description controller
Linstor controller manages linstor satellites and persistant data storage.


%files controller -f %{_builddir}/%{NAME_VERS}/controller/jar.deps
%dir %{LS_PREFIX}
%dir %{LS_PREFIX}/lib
%{LS_PREFIX}/lib/controller-%{FILE_VERSION}.jar
%dir %{LS_PREFIX}/bin
%{LS_PREFIX}/bin/Controller
%{LS_PREFIX}/bin/linstor-config
%{LS_PREFIX}/bin/linstor-database
%{LS_PREFIX}/bin/controller.postinst.sh
%{_unitdir}/linstor-controller.service
%{FIREWALLD_SERVICES}/linstor-controller.xml
%{_sysconfdir}/linstor/linstor.toml-example

%posttrans controller
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
%{LS_PREFIX}/lib/satellite-%{FILE_VERSION}.jar
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
* Thu Jan 23 2025 Rene Peinthor <rene.peinthor@linbit.com> 1.30.3-1
- New upstream release. Bug fixes

* Wed Dec 18 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.30.2-1
- New upstream release. Bug fixes

* Tue Dec 17 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.30.1-1
- New upstream release. Bug fixes

* Tue Dec 17 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.30.0-1
- New upstream release. Bug fixes

* Tue Nov  5 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.29.2-1
- New upstream release. Bug fixes

* Tue Sep 24 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.29.1-1
- New upstream release. Bug fixes

* Wed Jul 31 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.29.0-1
- New upstream release. Bug fixes

* Thu Jul 11 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.28.0-1
- New upstream release. Bug fixes

* Thu Apr 25 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.27.1-1
- New upstream release. Bug fixes

* Tue Apr  2 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.27.0-1
- New upstream release. Bug fixes

* Wed Feb 28 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.26.2-1
- New upstream release. Bug fixes

* Thu Feb 22 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.26.1-1
- New upstream release. Bug fixes

* Mon Jan 29 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.26.0-1
- New upstream release. Bug fixes

* Mon Jan 22 2024 Rene Peinthor <rene.peinthor@linbit.com> 1.26.0~rc.1-1
- New upstream release. Bug fixes

* Mon Nov 20 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.25.1-1
- New upstream release. Bug fixes

* Wed Oct 25 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.25.0-1
- New upstream release. Bug fixes

* Wed Oct 11 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.25.0~rc.1-1
- New upstream release. Bug fixes

* Wed Aug 30 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.24.2-1
- New upstream release. Bug fixes

* Thu Aug 10 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.24.1-1
- New upstream release. Bug fixes

* Mon Aug  7 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.24.0-1
- New upstream release. Bug fixes

* Mon Jul 24 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.24.0~rc.2-1
- New upstream release. Bug fixes

* Thu Jul 20 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.24.0~rc.1-1
- New upstream release. Bug fixes

* Tue May 23 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.23.0-1
- New upstream release. Bug fixes

* Thu Apr 27 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.22.1-1
- New upstream release. Bug fixes

* Mon Apr 17 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.22.0-1
- New upstream release. Bug fixes

* Wed Mar 22 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.21.1-1
- New upstream release. Bug fixes

* Tue Mar 14 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.21.0-1
- New upstream release. Bug fixes

* Tue Feb 28 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.21.0~rc.1-1
- New upstream release. Bug fixes

* Thu Jan 26 2023 Rene Peinthor <rene.peinthor@linbit.com> 1.20.3-1
- New upstream release. Bug fixes

* Wed Dec 14 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.20.2-1
- New upstream release. Bug fixes

* Tue Dec 13 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.20.1-1
- New upstream release. Bug fixes

* Tue Oct 18 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.20.0-1
- New upstream release. Bug fixes

* Tue Sep 20 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.20.0~rc.1-1
- New upstream release. Bug fixes

* Wed Jul 27 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.19.1-1
- New upstream release. Bug fixes

* Wed Jul 20 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.19.0-1
- New upstream release. Bug fixes

* Wed Jul  6 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.19.0~rc.1-1
- New upstream release. Bug fixes

* Mon May 23 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.18.2-1
- New upstream release. Bug fixes

* Thu May 12 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.18.1-1
- New upstream release. Bug fixes

* Mon May  2 2022 Gabor Hernadi <gabor.hernadi@linbit.com> 1.18.1~rc.2-1
- New upstream release. Bug fixes

* Mon Apr 25 2022 Moritz Wanzenböck <moritz.wanzenboeck@linbit.com> 1.18.1~rc.1-1
- New upstream release. Bug fixes

* Tue Mar 15 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.18.0-1
- New upstream release. Bug fixes

* Wed Feb 23 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.18.0~rc.3-1
- New upstream release. Bug fixes

* Tue Feb 22 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.18.0~rc.2-1
- New upstream release. Bug fixes

* Tue Feb 22 2022 Rene Peinthor <rene.peinthor@linbit.com> 1.18.0~rc.1-1
- New upstream release. Bug fixes

* Thu Dec  9 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.17.0-1
- New upstream release. Bug fixes

* Thu Nov 11 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.16.0-1
- New upstream release. Bug fixes

* Thu Nov  4 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.16.0~rc.3-1
- New upstream release. Bug fixes

* Thu Oct 14 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.16.0~rc.2-1
- New upstream release. Bug fixes

* Tue Oct 12 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.16.0~rc.1-1
- New upstream release. Kubernetes CRD support,  Bug fixes

* Thu Sep 23 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.15.0-1
- New upstream release. Bug fixes

* Tue Sep  7 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.15.0~rc.2-1
- New upstream release. Bug fixes

* Fri Aug 27 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.15.0~rc.1-1
- New upstream release. Bug fixes

* Tue Aug  3 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.14.0-1
- New upstream release. Bug fixes

* Mon Jul 19 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.14.0~rc.1-1
- New upstream release. Bug fixes

* Mon Jul 12 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.13.1-1
- New upstream release. Bug fixes

* Mon Jun 21 2021 Gabor Hernadi <gabor.hernadi@linbit.com> 1.13.0-1
- New upstream release. Bug fixes

* Mon Jun 14 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.13.0~rc.1-1
- New upstream release. Bcache support, Bug fixes

* Mon Jun  7 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.12.6-1
- New upstream release. Bug fixes

* Wed Jun  2 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.12.5-1
- New upstream release. Bug fixes

* Tue May 18 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.12.4-1
- New upstream release. Bug fixes

* Fri May  7 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.12.3-1
- New upstream release. Postgresql resource-group fixes. Bug fixes

* Tue May  4 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.12.2-1
- New upstream release. Fix db migrations

* Fri Apr 30 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.12.1-1
- New upstream release. Bug fixes

* Tue Apr 27 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.12.0-1
- New upstream release. Auto-verify-algo, exxos, Bug fixes

* Wed Jan 13 2021 Rene Peinthor <rene.peinthor@linbit.com> 1.11.1-1
- New upstream release. Bug fixes

* Fri Dec 18 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.11.0-1
- New upstream release. Snapshot port config, Bug fixes

* Mon Nov  9 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.10.0-1
- New upstream release. Auto-evict, etcd prefixes, Bug fixes.

* Wed Sep 23 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.9.0-1
- New upstream release. Auto-unplace, deletion API, bug fixes.

* Mon Aug 17 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.8.0-1
- New upstream release. Snapshot-shipping, Bug fixes.

* Wed Jul 22 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.7.3-1
- New upstream release. LvCreateOptions, Bug fixes.

* Mon Jul 13 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.7.2-1
- New upstream release. Bug fixes.

* Thu May 14 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.7.1-1
- New upstream release. Bug fixes.

* Thu May 07 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.7.0-1
- New upstream release. New auto-placer, snapshot view, bug fixes.

* Mon Apr 27 2020 Rene Peinthor <rene.peinthor@linbit.com> 1.6.2-1
- New upstream release. Fix query-max-volume-size, cache-layer fixes.

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
