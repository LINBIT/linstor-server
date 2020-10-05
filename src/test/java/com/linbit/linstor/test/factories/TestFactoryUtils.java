package com.linbit.linstor.test.factories;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFactoryUtils
{
    public static <T> List<T> copyOrNull(List<T> list)
    {
        if (list == null)
        {
            return null;
        }
        return new ArrayList<>(list);
    }

    public static <K, V> Map<K, V> copyOrNull(Map<K, V> map)
    {
        if (map == null)
        {
            return null;
        }
        return new HashMap<>(map);
    }

}
