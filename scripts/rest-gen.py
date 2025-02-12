#!/usr/bin/python3
import sys
import argparse
from collections import OrderedDict
from ruamel.yaml import YAML

yaml = YAML()


HDR_INFO = """/**
This file was generated with rest-gen.py, do not modify directly, the chances are high it is useless.
**/
"""


def print_header(stream, class_name, package, rest_version):
    print(HDR_INFO)
    print("package " + package + ";\n", file=stream)
    print("import com.linbit.linstor.annotation.Nullable;\n", file=stream)
    print("import java.util.List;", file=stream)
    print("import java.util.Map;", file=stream)
    print("import java.util.Collections;", file=stream)
    print(file=stream)
    print("import com.fasterxml.jackson.annotation.JsonInclude;\n", file=stream)

    print("public class " + class_name + "\n{", file=stream)
    print('    public static final String REST_API_VERSION = "{v}";\n'.format(v=rest_version))


def print_footer(stream, class_name):
    print("    private " + class_name + "()")
    print("    {")
    print("    }")
    print("}", file=stream)


def resolve_type_str(schema_lookup: OrderedDict, field_obj: OrderedDict):
    t = "FIXME"
    if "type" in field_obj:
        type_str = field_obj["type"]
        has_default = "default" in field_obj
        if type_str == "integer":
            if "format" in field_obj and field_obj["format"] == "int64":
                if has_default:
                    t = "long"
                else:
                    t = "Long"
            else:
                if has_default:
                    t = "int"
                else:
                    t = "Integer"
        elif type_str == "number":
            if has_default:
                t = "double"
            else:
                t = "Double"
        elif type_str == "string":
            t = "String"
        elif type_str == "boolean":
            if "default" in field_obj:
                t = "boolean"
            else:
                t = "Boolean"
        elif type_str == "array":
            t = "List<" + resolve_type_str(schema_lookup, field_obj['items']) + ">"
        elif type_str == "object":
            if "properties" not in field_obj:  # additionalProperties
                if "type" in field_obj["additionalProperties"] and \
                        field_obj["additionalProperties"]["type"] == "string":
                    t = "Map<String, String>"
                else:
                    t = "Map<String, " + resolve_type_str(schema_lookup, field_obj["additionalProperties"]) + ">"
            else:
                t = "object"  # needed for recursive call, will NOT go to java
    elif "$ref" in field_obj:
        assert field_obj["$ref"].startswith('#/components/schemas')
        schema_name = field_obj["$ref"][len('#/components/schemas/'):]
        t = resolve_type_str(schema_lookup, schema_lookup[schema_name])
        if t == "object":
            t = schema_name
    elif "oneOf" in field_obj:
        t = "Object"
    elif "allOf" in field_obj:
        t = "object"  # will resolve to schema_name
    else:
        raise RuntimeError("Unknown type for " + str(field_obj))
    return t

def is_field_deprecated(schema_lookup: OrderedDict, field_obj: OrderedDict):
    return "deprecated" in field_obj and field_obj["deprecated"]

def gen_javadoc(text: str, indent: str, indent_level: int):
    out = ''
    desc_lines = text.strip().split('\n')
    out += indent * indent_level + "/**\n"
    for line in desc_lines:
        text = indent * indent_level + " * " + line
        out += text.rstrip() + "\n"
    out += indent * indent_level + " */\n"
    return out


def gen_description_javadoc(field, indent: str, indent_level: int):
    out = ''
    if "description" in field:
        out += gen_javadoc(field["description"], indent, indent_level)
    return out


def value_to_string(val):
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)


def to_camel_case(snake_str):
    return ''.join(x.title() if i > 0 else x for i,x in enumerate(snake_str.split('_')))


def generate_class(schema_type: str, schema: OrderedDict, schema_lookup: OrderedDict):
    if "allOf" in schema or (schema["type"] == "object" and "properties" in schema):
        indent = "    "
    else:
        indent = "//    "

    out = gen_description_javadoc(schema, indent, 1)
    out += indent + "@JsonInclude(JsonInclude.Include.NON_EMPTY)\n"
    if schema.get("deprecated"):
        out += indent + "@Deprecated(forRemoval = true)\n"
    out += indent + "public static class " + schema_type + "\n"
    if "allOf" in schema:
        out += indent * 2 + "extends " + schema["allOf"][0]["$ref"][len('#/components/schemas/'):] + "\n"
        schema = schema["allOf"][1]
    out += indent + "{\n"
    if "properties" in schema:
        for fieldname in schema["properties"]:
            field = schema["properties"][fieldname]
            out += gen_description_javadoc(field, indent, 2)
            t = resolve_type_str(schema_lookup, field)
            depr = ""
            if schema.get("deprecated") or is_field_deprecated(schema_lookup, field):
                depr = "@Deprecated(forRemoval = true) "
            if t.startswith("Map"):
                dval = 'null' if "default" in field and field['default'] is None else 'Collections.emptyMap()'
                if dval == "null":
                    t = "@Nullable " + t
                out += indent * 2 + depr + "public {t} {n} = {v};\n".format(t=t, n=fieldname, v=dval)
            elif t.startswith("List"):
                dval = 'null' if "default" in field and field['default'] is None else 'Collections.emptyList()'
                if dval == "null":
                    t = "@Nullable " + t
                out += indent * 2 + depr + "public {t} {n} = {v};\n".format(t=t, n=fieldname, v=dval)
            else:
                if "default" in field:
                    dval = field['default']
                    if t == "String":
                        if dval is None:
                            out += indent * 2 + depr + 'public @Nullable {t} {n} = null;\n'.format(
                                t=t, n=fieldname
                            )
                        else:
                            out += indent * 2 + depr + 'public {t} {n} = "{v}";\n'.format(
                                t=t, n=fieldname, v=value_to_string(dval)
                            )
                    else:
                        out += indent * 2 + depr + "public {t} {n} = {v};\n".format(t=t, n=fieldname, v=value_to_string(dval))
                elif "required" in schema and fieldname in schema["required"]:
                    if t not in ["String", "Integer", "Long", "Double", "Boolean"]:
                        out += indent * 2 + depr + "public {t} {n} = new {v};\n".format(t=t, n=fieldname, v=t + "()")
                    elif t == "Integer":
                        out += indent * 2 + depr + "public int {n};\n".format(n=fieldname)
                    elif t == "String":
                        out += indent * 2 + depr + "public String {n};\n".format(n=fieldname)
                    else:
                        out += indent * 2 + depr + "public {t} {n};\n".format(t=t.lower(), n=fieldname)
                else:
                    out += indent * 2 + depr + "public @Nullable {t} {n};\n".format(t=t, n=fieldname)
    out += indent + "}\n"
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('rest_openapi_yaml')
    parser.add_argument('--package', default="com.linbit.linstor.api.rest.v1.serializer")
    parser.add_argument('--class-name', default="JsonGenTypes")

    args = parser.parse_args()

    with open(args.rest_openapi_yaml, 'r') as openapi_file:
        openapi_yaml = yaml.load(openapi_file)
        rest_version = openapi_yaml['info']['version']
        schemas = openapi_yaml['components']['schemas']
        print_header(sys.stdout, args.class_name, args.package, rest_version)
        class_list = []
        for schema in schemas:
            class_list.append(generate_class(schema, schemas[schema], schemas))

        print("\n".join(class_list))
        print_footer(sys.stdout, args.class_name)


if __name__ == "__main__":
    main()
