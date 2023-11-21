#!/usr/bin/python3

import os
import argparse
import subprocess
import socket
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("virter-manage")


def provision(args):
    subprocess.check_call(
        ["virter", "image", "build", args.distri,
         args.distri + "-linstor", "--vcpus", "2", "-p", "provision-" + args.distri.split('-')[0] + ".toml"])
    print("NOTE: add/set user_public_key in your ~/.config/virter/virter.toml")


def vms(args):
    for i in range(args.startnum, args.startnum + args.num):
        node = args.prefix + str(i)
        disk_args = ["--disk", "name=" + node + "_scratch,size=1GiB"]
        if args.distri.startswith('ubuntu'):
            disk_args += ["--disk", "name=" + node + "_zfs,size=1GiB"]
        subprocess.check_call(
            ["virter", "vm", "run", args.distri + "-linstor",
             "--id", str(20 + i),
             "--vcpus", "2",
             "-n", node,
             "--wait-ssh"] + disk_args)
        subprocess.check_call(["bash", "-c", "ssh-keyscan " + node + " >> ~/.ssh/known_hosts"])
        subprocess.check_call(["ssh", "root@" + node, "pvcreate /dev/vdb"])
        subprocess.check_call(["ssh", "root@" + node, "vgcreate scratch /dev/vdb"])

        if args.distri.startswith('ubuntu'):
            subprocess.check_call(["ssh", "root@" + node, "zpool create -f scratch-zfs /dev/vdc"])

        if args.distri.startswith('centos'):
            subprocess.check_call(["ssh", "root@" + node, "depmod -a"])


def remove(args):
    nodes = [args.prefix + str(i) for i in range(args.startnum, args.startnum)]
    for n in nodes:
        subprocess.check_call(["virter", "vm", "rm", n])
        subprocess.check_call(["ssh-keygen", "-R", n])
        ip = socket.gethostbyname(n)
        subprocess.check_call(["ssh-keygen", "-R", ip])

    if args.all:
        subprocess.check_call(["virter", "vm", "rm", "ubuntu-jammy-linstor", "centos-8-linstor"])


def hosts(args):
    startip: str = args.startip
    dot = startip.rfind('.')
    pre_ip = startip[:dot]
    post_ip = startip[dot+1:]

    for i in range(0, args.num):
        print("{pip}.{sip}\t\t{h}".format(pip=pre_ip, sip=str(int(post_ip)+i), h=args.prefix + str(i+1)))


def _add_vm_args(parser: argparse.ArgumentParser):
    parser.add_argument("-p", "--prefix", default="linstor", help="VM prefix name")
    parser.add_argument("-n", "--num", type=int, default=3, help="Number of vms")
    parser.add_argument("-s", "--startnum", type=int, default=1, help="Start index, e.g. expanding vms")

    return parser


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="sub-command help")

    p_provision = subparsers.add_parser("provision")
    p_provision.add_argument("distri", choices=["ubuntu-jammy", "centos-8"], help="distribution to provision")
    p_provision.set_defaults(func=provision)

    p_vms = subparsers.add_parser("vms")
    _add_vm_args(p_vms)
    p_vms.add_argument("distri", choices=["ubuntu-jammy", "centos-8"], help="distribution to provision")
    p_vms.set_defaults(func=vms)

    p_rm = subparsers.add_parser("rm")
    _add_vm_args(p_rm)
    p_rm.add_argument("-a", "--all", action="store_true", help="Remove also base images")
    p_rm.set_defaults(func=remove)

    p_hosts = subparsers.add_parser("gen-hosts")
    _add_vm_args(p_hosts)
    p_hosts.add_argument("startip", help="first virter ip: e.g. 192.168.125.21")
    p_hosts.set_defaults(func=hosts)

    args = parser.parse_args()
    if 'func' in args:
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
