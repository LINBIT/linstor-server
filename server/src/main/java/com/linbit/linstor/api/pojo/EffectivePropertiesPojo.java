package com.linbit.linstor.api.pojo;

import com.linbit.linstor.PriorityProps.MultiResult;
import com.linbit.linstor.PriorityProps.ValueWithDescription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class EffectivePropertiesPojo
{
    public final Map<String, EffectivePropertyPojo> properties;

    public EffectivePropertiesPojo(Map<String, EffectivePropertyPojo> propertiesRef)
    {
        properties = Collections.unmodifiableMap(propertiesRef);
    }

    public static EffectivePropertiesPojo build(Map<String, MultiResult> conflictingProps)
    {
        Map<String, EffectivePropertyPojo> propsMap = new TreeMap<>();
        for (Entry<String, MultiResult> propEntry : conflictingProps.entrySet())
        {
            List<PropPojo> props = new ArrayList<>();
            for (ValueWithDescription value : propEntry.getValue().conflictingList)
            {
                props.add(
                    new PropPojo(
                        value.objType.name(),
                        value.value,
                        value.objDescription
                    )
                );
            }
            ValueWithDescription active = propEntry.getValue().first;
            propsMap.put(
                propEntry.getKey(),
                new EffectivePropertyPojo(
                    new PropPojo(
                        active.objType.name(),
                        active.value,
                        active.objDescription
                    ),
                    props
                )
            );
        }
        return new EffectivePropertiesPojo(propsMap);
    }

    public static class EffectivePropertyPojo
    {
        public final PropPojo active;
        public final List<PropPojo> other;

        public EffectivePropertyPojo(PropPojo activeRef, List<PropPojo> otherRef)
        {
            active = activeRef;
            other = Collections.unmodifiableList(otherRef);
        }
    }

    public static class PropPojo
    {
        public final String type;
        public final String value;
        public final String descr;

        public PropPojo(String typeRef, String valueRef, String descrRef)
        {
            type = typeRef;
            value = valueRef;
            descr = descrRef;
        }
    }
}
