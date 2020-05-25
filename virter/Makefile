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

provision-centos: provision-centos.toml
	virter image build centos-8 centos-linstor -p $? --vcpus 2

define create_disks
	sudo virsh vol-create-as default $(1)_zfs.qcow2 1073741824 --format qcow2
endef

define prepare_vg
	ssh root@$(1) pvcreate /dev/sda
	ssh root@$(1) vgcreate scratch /dev/sda
endef

define attach_disks
	sudo virsh attach-disk $(1) `sudo virsh vol-list $(LIBVIRT_POOL) | grep $(1)_zfs.qcow2 | xargs | cut -d' ' -f2` vdb --subdriver qcow2 --persistent
	ssh root@$(1) zpool create -f scratch-zfs /dev/vdb
endef

define delete_disks
	-sudo virsh vol-delete $(1)_zfs.qcow2 --pool default
endef

vms-ubuntu:
	$(call create_disks, linstor1)
	$(call create_disks, linstor2)
	$(call create_disks, linstor3)
	virter vm run ubuntu-linstor --id 21 --vcpus 2 --wait-ssh -n linstor1
	virter vm run ubuntu-linstor --id 22 --vcpus 2 --wait-ssh -n linstor2
	virter vm run ubuntu-linstor --id 23 --vcpus 2 --wait-ssh -n linstor3
	ssh-keyscan linstor1 linstor2 linstor3 >> ~/.ssh/known_hosts
	$(call prepare_vg,linstor1)
	$(call prepare_vg,linstor2)
	$(call prepare_vg,linstor3)
	$(call attach_disks,linstor1)
	$(call attach_disks,linstor2)
	$(call attach_disks,linstor3)

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
	$(call delete_disks,linstor1)
	$(call delete_disks,linstor2)
	$(call delete_disks,linstor3)
	ssh-keygen -R linstor1
	ssh-keygen -R `getent hosts linstor1 | awk '{ print $$1 }'`
	ssh-keygen -R linstor2
	ssh-keygen -R `getent hosts linstor2 | awk '{ print $$1 }'`
	ssh-keygen -R linstor3
	ssh-keygen -R `getent hosts linstor3 | awk '{ print $$1 }'`

clean-all: clean
	virter vm rm ubuntu-linstor centos-linstor
