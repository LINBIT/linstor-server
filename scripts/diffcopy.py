#!/usr/bin/env python3

import os
import shutil
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--dry-run', action="store_true")
parser.add_argument('FOLDER_A')
parser.add_argument('FOLDER_B')
parser.add_argument('TARGET')

args = parser.parse_args()

# FOLDER_A is also the source, means every file that is extra in
# FOLDER_A is copied to TARGET

for d in (args.FOLDER_A, args.FOLDER_B):
    if not os.path.isdir(d):
        print(d + " is not a directory")
        sys.exit(1)


def in_folder(name):
    return [f for f in os.listdir(name) if os.path.isfile(os.path.join(name, f))]

distinct = [f for f in in_folder(args.FOLDER_A) if f not in in_folder(args.FOLDER_B)]

if len(distinct) == 0:
    sys.exit(0)

if not args.dry_run and os.path.isdir(args.TARGET):
    for f in distinct:
        shutil.copy(os.path.join(args.FOLDER_A, f), args.TARGET)
else:
    for f in distinct:
        print(os.path.join(args.TARGET, f))
