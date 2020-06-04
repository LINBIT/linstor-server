# you need /etc/hosts entries for linstor{1,2,3,4} and your virter ips

LIBVIRT_POOL=default

all: help

help:
	@echo "You can either provision ubuntu or centos vms with this Makefile using virter"
	@echo "For ubuntu:"
	@echo "  make provision-ubuntu"
	@echo "  make vms-ubuntu"
	@echo "For centos:"
	@echo "  make provision-centos"
	@echo "  make vms-centos"

provision-ubuntu: provision-ubuntu.toml
	virter image build ubuntu-focal ubuntu-linstor -p $? --vcpus 2
	@echo "NOTE: add/set user_public_key in your ~/.config/virter/virter.toml"

provision-centos: provision-centos.toml
	virter image build centos-8 centos-linstor -p $? --vcpus 2
	@echo "NOTE: add/set user_public_key in your ~/.config/virter/virter.toml"

define prepare_vg
	ssh root@$(1) pvcreate /dev/vdb
	ssh root@$(1) vgcreate scratch /dev/vdb
endef

define prepare_zfs
	ssh root@$(1) zpool create -f scratch-zfs /dev/vdc
endef

vms-ubuntu:
	virter vm run ubuntu-linstor --id 21 --vcpus 2 --wait-ssh -n linstor1 --disk name=linstor1_scratch,size=1GiB --disk name=linstor1_zfs,size=1GiB
	virter vm run ubuntu-linstor --id 22 --vcpus 2 --wait-ssh -n linstor2 --disk name=linstor2_scratch,size=1GiB --disk name=linstor2_zfs,size=1GiB
	virter vm run ubuntu-linstor --id 23 --vcpus 2 --wait-ssh -n linstor3 --disk name=linstor2_scratch,size=1GiB --disk name=linstor3_zfs,size=1GiB
	ssh-keyscan linstor1 linstor2 linstor3 >> ~/.ssh/known_hosts
	$(call prepare_vg,linstor1)
	$(call prepare_vg,linstor2)
	$(call prepare_vg,linstor3)
	$(call prepare_zfs,linstor1)
	$(call prepare_zfs,linstor2)
	$(call prepare_zfs,linstor3)

vms-centos:
	virter vm run centos-linstor --id 21 --vcpus 2 --wait-ssh -n linstor1
	virter vm run centos-linstor --id 22 --vcpus 2 --wait-ssh -n linstor2
	virter vm run centos-linstor --id 23 --vcpus 2 --wait-ssh -n linstor3
	ssh-keyscan linstor1 linstor2 linstor3 >> ~/.ssh/known_hosts
	$(call prepare_vg,linstor1)
	$(call prepare_vg,linstor2)
	$(call prepare_vg,linstor3)

	ssh root@linstor1 depmod -a
	ssh root@linstor2 depmod -a
	ssh root@linstor3 depmod -a

clean:
	virter vm rm linstor1
	virter vm rm linstor2
	virter vm rm linstor3
	ssh-keygen -R linstor1
	ssh-keygen -R `getent hosts linstor1 | awk '{ print $$1 }'`
	ssh-keygen -R linstor2
	ssh-keygen -R `getent hosts linstor2 | awk '{ print $$1 }'`
	ssh-keygen -R linstor3
	ssh-keygen -R `getent hosts linstor3 | awk '{ print $$1 }'`

clean-all: clean
	virter vm rm ubuntu-linstor centos-linstor
