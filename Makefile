GIT = git
MAKE = make
DOCKERREGISTRY := drbd.io
ARCH ?= amd64
ifneq ($(strip $(ARCH)),)
DOCKERREGISTRY := $(DOCKERREGISTRY)/$(ARCH)
endif
DOCKERREGPATH_CONTROLLER = $(DOCKERREGISTRY)/linstor-controller
DOCKERREGPATH_SATELLITE = $(DOCKERREGISTRY)/linstor-satellite
DOCKERFILE_CONTROLLER = Dockerfile.controller
DOCKERFILE_SATELLITE = Dockerfile.satellite
DOCKER_TAG ?= latest

GENRES=./server/generated-resources
GENSRC=./server/generated-src
VERSINFO=$(GENRES)/version-info.properties

# echo v0.1 to get it started
VERSION ?= $(shell echo $(shell git describe --tags || echo "v0.1") | sed -e 's/^v//;s/^[^0-9]*//;s/\(.*\)-g/\1-/')
GITHASH := $(shell git rev-parse HEAD)
DEBVERSION = $(shell echo $(VERSION) | sed -e 's/-/~/g')

.PHONY: .filelist
.filelist:
	@set -e ; submodules=`$(GIT) submodule foreach --quiet 'echo $$path'`; \
		$(GIT) ls-files | \
		grep -v "^\.gitlab" | \
		grep -vxF -e "$$submodules" | \
		sed '$(if $(PRESERVE_DEBIAN),,/^debian/d)' | \
		grep -v "gitignore\|gitmodules" > .filelist
	@$(GIT) submodule foreach --quiet 'git ls-files | sed -e "s,^,$$path/,"' | \
		grep -v "gitignore\|gitmodules" >> .filelist
	@[ -s .filelist ] # assert there is something in .filelist now
	@echo $(VERSINFO) >> .filelist
	@find $(GENSRC) -name '*.java' >> .filelist
	@echo libs >> .filelist
	@echo server/jar.deps >> .filelist
	@echo controller/jar.deps >> .filelist
	@echo satellite/jar.deps >> .filelist
	@echo jclcrypto/jar.deps >> .filelist
	@echo .gradlehome >> .filelist
	@echo gradlew >> .filelist
	@echo gradle/wrapper/gradle-wrapper.jar >> .filelist
	@echo gradle/wrapper/gradle-wrapper.properties >> .filelist
	@echo .filelist >> .filelist
	@echo "./.filelist updated."


tgz:
	test -s .filelist
	@if [ ! -e .git ]; then \
		echo >&2 "Not a git directory!"; exit 1; \
	fi; \
	tar --transform="s,^,linstor-server-$(VERSION)/,S"         \
	   --owner=0 --group=0 -czf - -T .filelist > linstor-server-$(VERSION).tar.gz

# we cannot use 'git submodule foreach':
# foreach only works if submodule already checked out
.PHONY: check-submods
check-submods:
	@if test -d .git && test -s .gitmodules; then \
		for d in `grep "^\[submodule" .gitmodules | cut -f2 -d'"'`; do \
			if [ ! "`ls -A $$d`" ]; then \
				git submodule init; \
				git submodule update; \
				break; \
			fi; \
		done; \
	fi

prepare_release: tarball

release: prepare_release

debrelease:
	$(MAKE) tarball PRESERVE_DEBIAN=1 KEEPNAME=1

.PHONY: check-all-committed
check-all-committed:
ifneq ($(FORCE),1)
	if ! tmp=$$(git diff --name-status HEAD 2>&1) || test -n "$$tmp" ; then \
		echo >&2 "$$tmp"; echo >&2 "Uncommitted changes"; exit 1; \
	fi
	if ! grep -q "^linstor-server ($(DEBVERSION)" debian/changelog ; then \
		echo >&2 "debian/changelog needs update"; exit 1; \
	fi
	for df in "$(DOCKERFILE_CONTROLLER)" "$(DOCKERFILE_SATELLITE)"; do \
		if test "$$(grep -c "ENV LINSTOR_VERSION $(VERSION)" "$$df")" -ne 2 ; then \
			echo >&2 "LINSTOR_VERSION in $$df needs update"; exit 1; \
		fi; \
	done
endif

.PHONY: getprotc
getprotc:
	@./gradlew getprotoc

.PHONY: gen-java
gen-java: getprotc
	@./gradlew generateJava
	@echo "generated java sources"

server/jar.deps: build.gradle
	./gradlew -q showServerRuntimeLibs > $@
ifneq ("$(wildcard libs/server-st.jar)","")
	echo "/usr/share/linstor-server/lib/server-st.jar" >> $@
endif

controller/jar.deps satellite/jar.deps jclcrypto/jar.deps: build.gradle
	@./gradlew installdist
	./scripts/diffcopy.py -n ./controller/build/install/controller/lib ./server/build/install/server/lib /usr/share/linstor-server/lib > controller/jar.deps
	sed -i '/^|usr|share|linstor-server|lib|server-/d' controller/jar.deps
	./scripts/diffcopy.py -n ./satellite/build/install/satellite/lib ./server/build/install/server/lib /usr/share/linstor-server/lib > satellite/jar.deps
	sed -i '/^|usr|share|linstor-server|lib|server-/d' satellite/jar.deps
	./scripts/diffcopy.py -n ./jclcrypto/build/install/jclcrypto/lib ./server/build/install/server/lib /usr/share/linstor-server/lib > jclcrypto/jar.deps
	sed -i '/^|usr|share|linstor-server|lib|server-/d' jclcrypto/jar.deps
ifneq ("$(wildcard libs/controller-st.jar)","")
	echo "/usr/share/linstor-server/lib/controller-st.jar" >> controller/jar.deps
endif
ifneq ("$(wildcard libs/satellite-st.jar)","")
	echo "/usr/share/linstor-server/lib/satellite-st.jar" >> satellite/jar.deps
endif
ifneq ("$(wildcard libs/jclcrypto-st.jar)","")
	echo "/usr/share/linstor-server/lib/jclcrypto-st.jar" >> jclcrypto/jar.deps
endif

tarball: check-all-committed check-submods versioninfo gen-java server/jar.deps controller/jar.deps satellite/jar.deps jclcrypto/jar.deps .filelist
	@./gradlew --no-daemon --gradle-user-home .gradlehome downloadDependencies
	rm -Rf .gradlehome/wrapper .gradlehome/native .gradlehome/.tmp .gradlehome/caches/[0-9]*
	mkdir -p ./libs
	$(MAKE) tgz

versioninfo:
	mkdir $(GENRES) || true
	echo "version=$(VERSION)" > $(VERSINFO)
	echo "git.commit.id=$(GITHASH)" >> $(VERSINFO)
	echo "build.time=$$(date -u --iso-8601=second)" >> $(VERSINFO)

.PHONY: dockerimage dockerimage.controller dockerimage.satellite dockerpatch update-rest-props resttypes
dockerimage.controller:
	docker build --build-arg=ARCH=$(ARCH) -f $(DOCKERFILE_CONTROLLER) -t $(DOCKERREGPATH_CONTROLLER):$(DOCKER_TAG) $(EXTRA_DOCKER_BUILDARGS) .
	docker tag $(DOCKERREGPATH_CONTROLLER):$(DOCKER_TAG) $(DOCKERREGPATH_CONTROLLER):latest

dockerimage.satellite:
	docker build --build-arg=ARCH=$(ARCH) -f $(DOCKERFILE_SATELLITE) -t $(DOCKERREGPATH_SATELLITE):$(DOCKER_TAG) $(EXTRA_DOCKER_BUILDARGS) .
	docker tag $(DOCKERREGPATH_SATELLITE):$(DOCKER_TAG) $(DOCKERREGPATH_SATELLITE):latest

ifneq ($(FORCE),1)
dockerimage: debrelease dockerimage.controller dockerimage.satellite
else
dockerimage: dockerimage.controller dockerimage.satellite
endif

dockerpath:
	@echo $(DOCKERREGPATH_CONTROLLER):latest $(DOCKERREGPATH_CONTROLLER):$(DOCKER_TAG) $(DOCKERREGPATH_SATELLITE):latest $(DOCKERREGPATH_SATELLITE):$(DOCKER_TAG)

update-rest-props:
	PYTHONPATH=../linstor-api-py ./scripts/rest-docu-props.py -i docs/rest_v1_openapi.yaml

resttypes:
	python3 ./scripts/rest-gen.py ./docs/rest_v1_openapi.yaml > controller/src/main/java/com/linbit/linstor/api/rest/v1/serializer/JsonGenTypes.java

generate-db-constants:
	./gradlew controller:distTar -PversionOverride=
	rm -Rf ./controller/build/distributions/controller
	tar -xf ./controller/build/distributions/controller.tar -C ./controller/build/distributions
	java -cp "controller/build/distributions/controller/lib/*" com/linbit/linstor/dbcp/GenerateSql

.PHONY: check-openapi
check-openapi: docs/rest_v1_openapi.yaml
	cd docs && docker run -it --rm -v `pwd`:/data jamescooke/openapi-validator:0.51.3 -e rest_v1_openapi.yaml
