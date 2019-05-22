#!/usr/bin/python3
import argparse
import sys
import tempfile
import shutil
from ruamel.yaml import scalarstring
from ruamel.yaml import YAML
from linstor.properties import properties

yaml = YAML()


class DescriptionEntry:
    def __init__(self, path, method):
        self._path = path
        self._method = method
        self._description = ''

    @property
    def path(self):
        return self._path

    @property
    def method(self):
        return self._method

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, desc):
        self._description = desc


def format_description(section: str, property_dict) -> str:
    desc = ''
    props = property_dict[section]
    for prop in props:
        s = '- `' + prop['key'] + '` - '
        _type = prop['type']
        type_details = ""
        if _type == 'symbol':
            type_short = 'enum'
            type_details = '    * ' + "\n    * ".join(prop['values']) + "\n"
        elif _type == 'numeric-or-symbol':
            type_short = 'enum [`' + str(prop['min']) + "-" + str(prop['max']) + "`]"
            type_details = "    * " + "\n    * ".join(prop['values']) + "\n"
        elif _type == 'range':
            type_short = _type + "[`" + str(prop['min']) + "-" + str(prop['max']) + "`]" + prop.get(' unit', '')
        elif _type == 'regex':
            type_short = _type + "[`" + str(prop['value']) + "`]"
        else:
            type_short = _type
        desc += s + type_short + "\n"
        doc = prop.get("info", "")
        if doc:
            desc += "\n    " + doc + "\n\n"
        desc += type_details + "\n" if type_details else ""
    return desc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--in-place', action="store_true", help="edit input file")
    parser.add_argument('openapi')

    args = parser.parse_args()

    mapping = {
        'controller': DescriptionEntry('/v1/controller/properties', 'post'),
        'node': DescriptionEntry('/v1/nodes/{node}', 'put'),
        'storagepool': DescriptionEntry('/v1/nodes/{node}/storage-pools/{storagepool}', 'put'),
        'resource-definition': DescriptionEntry('/v1/resource-definitions/{resource}', 'put'),
        'volume-definition': DescriptionEntry(
            '/v1/resource-definitions/{resource}/volume-definitions/{volume_number}',
            'put'
        ),
        'resource': DescriptionEntry('/v1/resource-definitions/{resource}/resources/{node}', 'put'),
        'rsc-conn': DescriptionEntry(
            '/v1/resource-definitions/{resource}/resource-connections/{node_a}/{node_b}',
            'put'
        ),
        'drbd-proxy': DescriptionEntry('/v1/resource-definitions/{resource}/drbd-proxy', 'put')
    }

    out_name = None
    with open(args.openapi, 'r') as openapi_file:
        openapi_yaml = yaml.load(openapi_file)
        for section in properties.keys():
            if section.startswith('drbd-proxy'):
                mapping['drbd-proxy'].description += format_description(section, properties)
            elif section in mapping:
                mapping[section].description = format_description(section, properties)

        for section in mapping.keys():
            desc_entry = mapping[section]
            desc_entry.description = 'Sets or modifies properties\n\nPossible properties are:\n' +\
                                     desc_entry.description
            openapi_yaml['paths'][desc_entry.path][desc_entry.method]['description'] = \
                scalarstring.PreservedScalarString(desc_entry.description)

        yaml.indent(mapping=2, sequence=4, offset=2)
        if args.in_place:
            with open(tempfile.mktemp(), "w+") as outfile:
                out_name = outfile.name
                yaml.dump(openapi_yaml, outfile)
        else:
            yaml.dump(openapi_yaml, sys.stdout)

    if out_name and args.in_place:
        shutil.copyfile(out_name, args.openapi)


if __name__ == "__main__":
    main()
