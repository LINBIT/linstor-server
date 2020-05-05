# you need /etc/hosts entries for linstor{1,2,3,4} and your virter ips

all: provision

provision: provision.toml
	virter image build ubuntu-focal ubuntu-linstor --id 100 -p provision.toml --vcpus 2

define create_disks
	sudo virsh vol-create-as default $(1)_zfs.qcow2 1073741824 --format qcow2
endef

define attach_disks
	sudo virsh attach-disk $(1) /mnt/data/vms/$(1)_zfs.qcow2 vdb --subdriver qcow2 --persistent
	ssh root@$(1) pvcreate /dev/sda
	ssh root@$(1) vgcreate scratch /dev/sda
	ssh root@$(1) zpool create -f scratch-zfs /dev/vdb
endef

define delete_disks
	-sudo virsh vol-delete $(1)_zfs.qcow2 --pool default
endef

vms:
	$(call create_disks, linstor1)
	$(call create_disks, linstor2)
	$(call create_disks, linstor3)
	virter vm run ubuntu-linstor --id 21 --vcpus 2 --wait-ssh -n linstor1
	virter vm run ubuntu-linstor --id 22 --vcpus 2 --wait-ssh -n linstor2
	virter vm run ubuntu-linstor --id 23 --vcpus 2 --wait-ssh -n linstor3
	ssh-keyscan linstor1 linstor2 linstor3 >> ~/.ssh/known_hosts
	$(call attach_disks,linstor1)
	$(call attach_disks,linstor2)
	$(call attach_disks,linstor3)

clean:
	virter vm rm linstor1
	virter vm rm linstor2
	virter vm rm linstor3
	$(call delete_disks, linstor1)
	$(call delete_disks, linstor2)
	$(call delete_disks, linstor3)
	ssh-keygen -R linstor1
	#ssh-keygen -R 192.168.125.21
	ssh-keygen -R linstor2
	#ssh-keygen -R 192.168.125.22
	ssh-keygen -R linstor3
	#ssh-keygen -R 192.168.125.23

clean-all: clean
	virter vm rm ubuntu-linstor
