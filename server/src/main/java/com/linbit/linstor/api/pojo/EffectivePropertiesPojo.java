package com.linbit.linstor.api.pojo;

import com.linbit.linstor.PriorityProps.MultiResult;
import com.linbit.linstor.PriorityProps.ValueWithDescription;
import com.linbit.linstor.api.prop.LinStorObject;

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
                        compatLinstorObjToString(value.objType),
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
                        compatLinstorObjToString(active.objType),
                        active.value,
                        active.objDescription
                    ),
                    props
                )
            );
        }
        return new EffectivePropertiesPojo(propsMap);
    }

    /**
     * Old clients try to rename the lengthy "RESOURCE" to "R" in the context of "Skip-Disk (R)". However, since we
     * renamed the enums from "RESOURCE" to "RSC", the current client gets a "KeyError 'RSC'".
     * To prevent this, we map the new "RSC" back to the previous "RESOURCE".
     * We also patch the client to map both, "RESOURCE" as well as "RSC" to "R".
     * Once we declare the current client version to be "old enough" to drop support, we can also delete this compat
     * method and simply use <code>objType.name()</code> instead
     *
     * @param linstorObjRef
     *
     * @return
     */
    @Deprecated
    private static String compatLinstorObjToString(LinStorObject linstorObjRef)
    {
        String ret;
        switch (linstorObjRef)
        {
            // we do not need to rename all enum values, just the ones that the current client already knows and tries
            // to map
            case STLT:
                ret = "SATELLITE";
                break;
            case NODE:
                ret = "NODE";
                break;
            case RSC_DFN:
                ret = "RESOURCE_DEFINITION";
                break;
            case RSC:
                ret = "RESOURCE";
                break;
            case STOR_POOL:
                ret = "STORAGEPOOL";
                break;
            // $CASES-OMITTED$
            default:
                ret = linstorObjRef.name();
                break;
        }
        return ret;
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
