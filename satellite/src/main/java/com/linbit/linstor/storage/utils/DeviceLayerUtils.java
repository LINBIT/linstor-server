package com.linbit.linstor.storage.utils;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.VlmStorageState;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.utils.ExceptionThrowingBiConsumer;
import com.linbit.utils.ExceptionThrowingConsumer;
import com.linbit.utils.ExceptionThrowingFunction;

import java.util.Iterator;
import java.util.Map;

public class DeviceLayerUtils
{
    public static <RSC extends AbsResource<RSC>> void foreachVlm(
        RSC[] rscs,
        ExceptionThrowingConsumer<AbsVolume<RSC>, StorageException> func
    )
        throws StorageException
    {
        for (final RSC rsc : rscs)
        {
            final Iterator<? extends AbsVolume<RSC>> vlmIt = rsc.iterateVolumes();
            while (vlmIt.hasNext())
            {
                func.accept(vlmIt.next());
            }
        }
    }

    public static <RSC extends AbsResource<RSC>> void foreachVlm(
        RSC[] rscs,
        Map<AbsVolume<RSC>, VlmStorageState> states,
        ExceptionThrowingBiConsumer<AbsVolume<RSC>, VlmStorageState, StorageException> func
    )
        throws StorageException
    {
        for (final RSC rsc : rscs)
        {
            final Iterator<? extends AbsVolume<RSC>> vlmIt = rsc.iterateVolumes();
            while (vlmIt.hasNext())
            {
                func.accept(vlmIt.next(), states.get(vlmIt.next()));
            }
        }
    }

    public static double checkedGet(Map<String, String> map, String key, double defaultValue)
        throws StorageException
    {
        return checkedGet(map, key, defaultValue, Double::parseDouble, "double");
    }

    public static int checkedGet(Map<String, String> map, String key, int defaultValue)
        throws StorageException
    {
        return checkedGet(map, key, defaultValue, Integer::parseInt, "integer");
    }

    public static long checkedGet(Map<String, String> map, String key, long defaultValue)
        throws StorageException
    {
        return checkedGet(map, key, defaultValue, Long::parseLong, "long");
    }

    public static String checkedGet(Map<String, String> map, String key, String defaultValue)
        throws StorageException
    {
        return checkedGet(map, key, defaultValue, str -> str, "string");
    }

    public static <R, EXC extends Exception> R checkedGet(
        Map<String, String> map,
        String key,
        R defaultValue,
        ExceptionThrowingFunction<String, R, EXC> parserFunc,
        String parseTargetDescription
    )
        throws StorageException
    {
        R ret = defaultValue;
        String valStr = map.get(key);
        if (valStr != null)
        {
            try
            {
                ret = parserFunc.accept(valStr);
            }
            catch (Exception exc)
            {
                throw new StorageException(
                    "Failed to parse value '" + valStr + "' from key '" + key + "' as " + parseTargetDescription,
                    exc
                );
            }
        }
        return ret;
    }

    public static Props getNamespaceStorDriver(Props props)
    {
        return props.getNamespace(DeviceLayer.STOR_DRIVER_NAMESPACE).orElse(ReadOnlyProps.emptyRoProps());
    }

    public static Props getInternalNamespaceStorDriver(Props props)
    {
        return props.getNamespace(DeviceProvider.STORAGE_NAMESPACE).orElse(ReadOnlyProps.emptyRoProps());
    }

    private DeviceLayerUtils()
    {
    }
}
