package com.linbit.utils;

import java.util.HashMap;
import java.util.Map;

public class ClassMap<T>
{
    private final Map<Class<? extends T>, Object> map;

    public ClassMap()
    {
        this(new HashMap<>());
    }

    public ClassMap(HashMap<Class<? extends T>, Object> backingMap)
    {
        map = backingMap;
    }

    public <VAL> VAL put(VAL value)
    {
        return (VAL) map.put(((Class<T>) value.getClass()), value);
    }

    public <T> T get(Class<T> clazz)
    {
        return clazz.cast(map.get(clazz));
    }
}
