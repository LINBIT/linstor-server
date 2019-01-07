package com.linbit.linstor.storage.layer;

import com.linbit.linstor.storage.layer.data.categories.LayerData;

import java.util.Map;

/**
 * Heterogeneous typesafe container.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 * @param <T>
 */
public class LayerDataStorage<T extends LayerData>
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
