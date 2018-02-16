package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.proto.LinStorMapEntryOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ProtoMapUtils
{
    public static Map<String, String> asMap(List<LinStorMapEntryOuterClass.LinStorMapEntry> list)
    {
        Map<String, String> map = new TreeMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : list)
        {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public static List<LinStorMapEntryOuterClass.LinStorMapEntry> fromMap(Map<String, String> map)
    {
        List<LinStorMapEntryOuterClass.LinStorMapEntry> entries = new ArrayList<>(map.size());
        for (Map.Entry<String, String> entry : map.entrySet())
        {
            entries.add(LinStorMapEntryOuterClass.LinStorMapEntry.newBuilder()
                .setKey(entry.getKey())
                .setValue(entry.getValue())
                .build()
            );
        }
        return entries;
    }

    private ProtoMapUtils()
    {
    }
}
