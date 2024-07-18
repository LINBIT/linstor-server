package com.linbit.linstor.utils;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.propscon.ReadOnlyProps;

import java.util.function.BiFunction;
import java.util.function.Function;

public class PropsUtils
{
    public static String getPropOrEnv(ReadOnlyProps props, String firstKey, String envKey)
    {
        return getPropOrEnv(props::getProp, firstKey, null, envKey, null);
    }

    public static String getPropOrEnv(
        ReadOnlyProps props,
        String firstKey,
        String firstNamespace,
        String envKey,
        String envNamespace
    )
    {
        return getPropOrEnv(props::getProp, firstKey, firstNamespace, envKey, envNamespace);
    }

    public static String getPropOrEnv(PriorityProps prioProps, String firstKey, String envKey)
    {
        return getPropOrEnv(prioProps::getProp, firstKey, null, envKey, null);
    }

    public static String getPropOrEnv(
        PriorityProps prioProps,
        String firstKey,
        String firstNamespace,
        String envKey,
        String envNamespace
    )
    {
        return getPropOrEnv(prioProps::getProp, firstKey, firstNamespace, envKey, envNamespace);
    }

    public static String getPropOrEnv(
        BiFunction<String, String, String> getterFunc,
        String firstKey,
        String firstNamespace,
        String envKey,
        String envNamespace
    )
    {
        String val = getterFunc.apply(firstKey, firstNamespace);
        if (val == null)
        {
            String env = getterFunc.apply(envKey, envNamespace);
            if (env != null)
            {
                val = System.getenv(env);
            }
        }
        return val;
    }

    public static String getPropOrEnv(ReadOnlyProps propsRef, String[] prioKeysRef, String[] prioEnvKeysRef)
    {
        return getPropOrEnv(propsRef::getProp, prioKeysRef, prioEnvKeysRef);
    }

    public static String getPropOrEnv(PriorityProps prioPropsRef, String[] prioKeysRef, String[] prioEnvKeysRef)
    {
        return getPropOrEnv(prioPropsRef::getProp, prioKeysRef, prioEnvKeysRef);
    }

    public static String getPropOrEnv(
        Function<String, String> getProp,
        String[] prioKeysRef,
        String[] prioEnvKeysRef
    )
    {
        String val = null;
        for (String key : prioKeysRef)
        {
            val = getProp.apply(key);
            if (val != null)
            {
                break;
            }
        }
        if (val == null)
        {
            for (String envKey : prioEnvKeysRef)
            {
                String env = getProp.apply(envKey);
                if (env != null)
                {
                    val = System.getenv(env);
                    if (val != null)
                    {
                        break;
                    }
                }
            }
        }
        return val;
    }
}
