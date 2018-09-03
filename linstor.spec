Name: linstor
Version: 0.6.1
Release: 1%{?dist}
Summary: LINSTOR SDS
%define GRADLE_TASKS installdist
%define GRADLE_FLAGS --offline --gradle-user-home /tmp --no-daemon --exclude-task generateJava
%define LS_PREFIX /usr/share/linstor-server
%define NAME_VERS %{name}-server-%{version}

Group: System Environment/Daemons
License: GPLv2+
URL: https://github.com/LINBIT/linstor-server
Source0: http://www.linbit.com/downloads/linstor/linstor-server-%{version}.tar.gz

BuildRequires: java-1.8.0-openjdk-headless java-1.8.0-openjdk-devel python

%description
TODO.


%prep
%setup -q -n %{NAME_VERS}


%build
rm -rf ./build/install
gradle %{GRADLE_TASKS} %{GRADLE_FLAGS}

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

%post controller
%{LS_PREFIX}/bin/controller.postinst.sh
%systemd_post linstor-controller.service

%preun controller
%systemd_preun linstor-controller.service

### satellite
%package satellite
Summary: Linstor satellite specific files
Requires: linstor-common = %{version}
Requires: lvm2
Requires: drbd-utils

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

%post satellite
%systemd_post linstor-satellite.service

%preun satellite
%systemd_preun linstor-satellite.service

%changelog
* Mon Jul 30 2018 Roland Kammerer <roland.kammerer@linbit.com> 0.2.6-1
- New upstream release.
