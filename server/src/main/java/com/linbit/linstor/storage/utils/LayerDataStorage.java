package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.interfaces.categories.LayerObject;

import java.util.Map;

/**
 * Heterogeneous typesafe container.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 * @param <T>
 */
public class LayerDataStorage<T extends LayerObject>
{
    private final Map<Class<? extends T>, T> layerData;

    public LayerDataStorage(Map<Class<? extends T>, T> backingMap)
    {
        layerData = backingMap;
    }

    @SuppressWarnings("unchecked")
    public <DATA extends T> DATA put(DATA data)
    {
        Class<DATA> clazz = (Class<DATA>) data.getClass();
        DATA ret = (DATA) layerData.get(clazz);
        layerData.put(clazz, data);
        return ret;
    }

    @SuppressWarnings("unchecked")
    public <DATA extends T> DATA get(Class<DATA> dataClass)
    {
        return (DATA) layerData.get(dataClass);
    }
}
