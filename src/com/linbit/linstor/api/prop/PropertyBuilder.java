package com.linbit.linstor.api.prop;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.Property.PropertyType;
import com.linbit.linstor.propscon.Props;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PropertyBuilder
{
    private String name;
    private String key;
    private PropertyType type;
    private String value;
    private boolean internal;
    private String info;
    private String[] values;
    private long max;
    private long min;

    public PropertyBuilder()
    {
    }

    public Property build()
    {
        Property prop = null;

        switch (type)
        {
            case REGEX:
                prop = new RegexProperty(name, key, value, internal, info);
                break;
            case SYMBOL:
                prop = new RegexProperty(name, key, buildValuesEnumRegex(), internal, info);
                break;
            case BOOLEAN:
                prop = new RegexProperty(
                    name,
                    key,
                    "(?i)(?:true|false|yes|no)",
                    internal,
                    info
                );
                break;
            case RANGE:
                prop = new RangeProperty(name, key, min, max, internal, info);
                break;
            case STRING:
                prop = new StringProperty(name, key, internal, info);
                break;
            case NUMERIC_OR_SYMBOL:
                prop = new NumericOrSymbolProperty(name, key, min, max, buildValuesEnumRegex(), internal, info);
                break;
            default:
                throw new ImplementationError("Unknown property type: " + type, null);
        }
        return prop;
    }

    private String buildValuesEnumRegex()
    {
        StringBuilder symbolValue = new StringBuilder("(?:");
        for (String val : values)
        {
            symbolValue.append(val).append("|");
        }
        symbolValue.setLength(symbolValue.length() - 1);
        symbolValue.append(")");
        return symbolValue.toString();
    }

    public PropertyBuilder name(String nameRef)
    {
        name = nameRef;
        return this;
    }

    public PropertyBuilder keyRef(String... keyRef)
    {
        List<String> keyList = new ArrayList<>();
        for (String keyPart : keyRef)
        {
            try
            {
                keyList.add(
                    (String) ApiConsts.class.getField(keyPart).get(null)
                );
            }
            catch (Exception exc)
            {
                throw new LinStorRuntimeException(
                    "Failed to generate whitelist for properties.",
                    "An error occured while resolving the API property key: '" + keyPart,
                    null, // cause
                    null, // correction
                    "Rule name: " + name + ".\nOrig keys: " + Arrays.toString(keyRef),
                    exc
                );
            }
        }
        key = keyList.stream().collect(Collectors.joining(Props.PATH_SEPARATOR));
        return this;
    }

    public PropertyBuilder keyStr(String keyStr)
    {
        key = keyStr;
        return this;
    }

    public PropertyBuilder type(String typeRef)
    {
        type = PropertyType.valueOfIgnoreCase(typeRef);
        if (type == null)
        {
            throw new NullPointerException("Unknown type '" + typeRef + "'");
        }
        return this;
    }

    public PropertyBuilder value(String valueRef)
    {
        value = valueRef;
        return this;
    }

    public PropertyBuilder internal(String internalRef)
    {
        internal = "true".equalsIgnoreCase(internalRef);
        return this;
    }

    public PropertyBuilder info(String infoRef)
    {
        info = infoRef;
        return this;
    }

    public PropertyBuilder values(String... valuesRef)
    {
        values = valuesRef;
        return this;
    }

    public PropertyBuilder max(String maxStr)
    {
        max = Long.parseLong(maxStr);
        return this;
    }

    public PropertyBuilder min(String minStr)
    {
        min = Long.parseLong(minStr);
        return this;
    }

    public static void main(String[] args)
    {
        GeneratedPropertyRules.getWhitelistedProperties();
    }
}
