package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.ExceptionThrowingFunction;

import java.util.Map;

public class ConfigParseUtils
{
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
}
