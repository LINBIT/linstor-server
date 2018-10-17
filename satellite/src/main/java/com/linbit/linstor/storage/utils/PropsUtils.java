package com.linbit.linstor.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;

public class PropsUtils
{
    public static boolean useDmStats(Props props)
    {
        return asBool(getConstantKey(props, ApiConsts.KEY_DMSTATS, ApiConsts.NAMESPC_STORAGE_DRIVER));
    }

    private static String getConstantKey(Props props, String key, String namespc)
    {
        String ret;
        try
        {
            ret = props.getProp(key, namespc);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Hardcoded property key is invalid", exc);
        }
        return ret;
    }

    private static boolean asBool(String boolStr)
    {
        return boolStr != null && Boolean.valueOf(boolStr);
    }
}
