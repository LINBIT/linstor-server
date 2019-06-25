FROM centos:centos7 as builder

ENV LINSTOR_VERSION 0.9.12

ENV GRADLE_VERSION 4.4.1

ENV LINSTOR_TGZNAME linstor-server
ENV LINSTOR_TGZ ${LINSTOR_TGZNAME}-${LINSTOR_VERSION}.tar.gz

USER root
RUN yum -y update-minimal --security --sec-severity=Important --sec-severity=Critical
RUN groupadd makepkg # !lbbuild
RUN useradd -m -g makepkg makepkg # !lbbuild

RUN yum install -y sudo # !lbbuild
RUN usermod -a -G wheel makepkg # !lbbuild

RUN yum install -y rpm-build wget unzip which make git java-1.8.0-openjdk-devel && yum clean all -y # !lbbuild
RUN rpm -e --nodeps fakesystemd && yum install -y systemd && yum clean all -y || true # !lbbuild
RUN wget --quiet https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-all.zip -O /tmp/gradle.zip && mkdir /opt/gradle && unzip -d /opt/gradle /tmp/gradle.zip && rm -f /tmp/gradle.zip # !lbbuild

RUN mkdir -p /tmp/linstor-$LINSTOR_VERSION
# one can not comment COPY
RUN cd /tmp && wget https://www.linbit.com/downloads/linstor/$LINSTOR_TGZ # !lbbuild
# =lbbuild COPY /${LINSTOR_TGZ} /tmp/

USER makepkg

RUN cd ${HOME} && \
  cp /tmp/${LINSTOR_TGZ} ${HOME} && \
  mkdir -p ${HOME}/rpmbuild/SOURCES && \
  cp /tmp/${LINSTOR_TGZ} ${HOME}/rpmbuild/SOURCES && \
  tar xvf ${LINSTOR_TGZ} && \
  cd ${LINSTOR_TGZNAME}-${LINSTOR_VERSION} && \
  PATH=/opt/gradle/gradle-$GRADLE_VERSION/bin:$PATH rpmbuild -bb --define "debug_package %{nil}"  linstor.spec

FROM quay.io/linbit/drbd-utils
# this is/needs to be based on registry.access.redhat.com/ubi7/ubi

ENV LINSTOR_VERSION 0.9.12
ARG release=1

LABEL name="linstor-satellite" \
      vendor="LINBIT" \
      version="$LINSTOR_VERSION" \
      release="$release" \
      summary="LINSTOR's satellite component" \
      description="LINSTOR's satellite component"

COPY COPYING /licenses/gpl-3.0.txt

COPY --from=builder /home/makepkg/rpmbuild/RPMS/noarch/*.rpm /tmp/
RUN yum -y update-minimal --security --sec-severity=Important --sec-severity=Critical && \
  yum install -y which lvm2 && yum install -y /tmp/linstor-common*.rpm /tmp/linstor-satellite*.rpm && yum clean all -y

RUN sed -i 's/udev_rules.*=.*/udev_rules=0/' /etc/lvm/lvm.conf

EXPOSE 3366/tcp 3367/tcp
ENTRYPOINT ["/usr/share/linstor-server/bin/Satellite", "--logs=/var/log/linstor-satellite", "--config-directory=/etc/linstor", "--skip-hostname-check"]
