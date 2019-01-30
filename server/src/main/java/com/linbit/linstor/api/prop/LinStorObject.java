package com.linbit.linstor.api.prop;

import com.linbit.linstor.api.ApiConsts;

public enum LinStorObject
{
    NODE,
    NET_IF,
    NODE_CONN,
    RESOURCE_DEFINITION,
    RESOURCE,
    RSC_CONN,
    VOLUME_DEFINITION,
    VOLUME,
    VOLUME_CONN,
    CONTROLLER,
    STORAGEPOOL,
    STORAGEPOOL_DEFINITION,
    SNAPSHOT,
    KVS,
    // The various DRBD Proxy configuration sections are considered separate objects for the purposes of setting
    // properties, even though the properties are stored in the resource definition
    DRBD_PROXY,
    DRBD_PROXY_ZLIB,
    DRBD_PROXY_LZMA,
    DRBD_PROXY_LZ4;

    LinStorObject()
    {
    }

    public static LinStorObject drbdProxyCompressionObject(String compressionType)
    {
        LinStorObject linStorObject;
        switch (compressionType)
        {
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_ZLIB:
                linStorObject = LinStorObject.DRBD_PROXY_ZLIB;
                break;
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_LZMA:
                linStorObject = LinStorObject.DRBD_PROXY_LZMA;
                break;
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_LZ4:
                linStorObject = LinStorObject.DRBD_PROXY_LZ4;
                break;
            default:
                linStorObject = null;
                break;
        }
        return linStorObject;
    }
}
