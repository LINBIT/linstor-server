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


FROM registry.access.redhat.com/ubi7/ubi
MAINTAINER Roland Kammerer <roland.kammerer@linbit.com>

# ENV can not be shared between builder and "main"
ENV LINSTOR_VERSION 0.9.12
ARG release=1

LABEL name="linstor-controller" \
      vendor="LINBIT" \
      version="$LINSTOR_VERSION" \
      release="$release" \
      summary="LINSTOR's controller component" \
      description="LINSTOR's controller component"

COPY COPYING /licenses/gpl-3.0.txt

COPY --from=builder /home/makepkg/rpmbuild/RPMS/noarch/*.rpm /tmp/
RUN yum -y update-minimal --security --sec-severity=Important --sec-severity=Critical && \
  yum install -y which && yum install -y /tmp/linstor-common*.rpm /tmp/linstor-controller*.rpm && yum clean all -y

RUN groupadd linstor
RUN useradd -m -g linstor linstor
RUN mkdir /var/log/linstor-controller
RUN chown -R root:linstor /etc/linstor /var/lib/linstor /var/log/linstor-controller
RUN chmod g+w /etc/linstor /var/lib/linstor /var/log/linstor-controller

EXPOSE 3376/tcp 3377/tcp 3370/tcp
USER linstor
ENTRYPOINT ["/usr/share/linstor-server/bin/Controller", "--logs=/var/log/linstor-controller", "--config-directory=/etc/linstor", "--rest-bind=0.0.0.0:3370"]
