package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class CtrlPropsInfoApiCallHandler
{
    private final WhitelistProps whitelistProps;

    @Inject
    CtrlPropsInfoApiCallHandler(
        WhitelistProps whitelistPropsRef
    )
    {
        whitelistProps = whitelistPropsRef;
    }

    // Note: JsonGenTypes was used fully knowing that it shouldn't be used here,
    // however, any alternative would have been unnecessarily complicated

    public Map<String, Map<String, JsonGenTypes.PropsInfo>> listAllProps()
    {
        Map<LinStorObject, Map<String, Property>> rules = whitelistProps.getRules();
        Map<String, Map<String, JsonGenTypes.PropsInfo>> propsInfo = new HashMap<>();

        for (LinStorObject obj : rules.keySet())
        {
            propsInfo.put(obj.name(), listFilteredProps(obj));
        }
        return propsInfo;
    }

    public Map<String, JsonGenTypes.PropsInfo> listFilteredProps(LinStorObject obj)
    {
        Map<LinStorObject, Map<String, Property>> rules = whitelistProps.getRules();
        Map<String, JsonGenTypes.PropsInfo> propsInfo = new HashMap<>();

        for (Property prop : rules.get(obj).values())
        {
            Property.PropertyType type = prop.getType();
            JsonGenTypes.PropsInfo info = new JsonGenTypes.PropsInfo();
            info.info = prop.getInfo();
            info.prop_type = type.toString().toLowerCase();
            if (!(type.equals(Property.PropertyType.LONG) ||
                type.equals(Property.PropertyType.STRING)))
            {
                info.value = prop.getValue();
            }
            info.dflt = prop.getDflt();
            info.unit = prop.getUnit();
            propsInfo.put(prop.getKey(), info);
        }
        return propsInfo;
    }
}
