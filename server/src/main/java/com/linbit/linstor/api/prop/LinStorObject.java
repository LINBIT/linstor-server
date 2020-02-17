package com.linbit.linstor.api.prop;

import com.linbit.linstor.api.ApiConsts;

public enum LinStorObject
{
    NODE(ApiConsts.MASK_NODE),
    NET_IF(ApiConsts.MASK_NET_IF),
    NODE_CONN(ApiConsts.MASK_NODE_CONN),
    RESOURCE_DEFINITION(ApiConsts.MASK_RSC_DFN),
    RESOURCE(ApiConsts.MASK_RSC),
    RSC_CONN(ApiConsts.MASK_RSC_CONN),
    VOLUME_DEFINITION(ApiConsts.MASK_VLM_DFN),
    VOLUME(ApiConsts.MASK_VLM),
    VOLUME_CONN(ApiConsts.MASK_VLM_CONN),
    CONTROLLER(ApiConsts.MASK_CTRL_CONF),
    STORAGEPOOL(ApiConsts.MASK_STOR_POOL),
    STORAGEPOOL_DEFINITION(ApiConsts.MASK_STOR_POOL_DFN),
    SNAPSHOT(ApiConsts.MASK_SNAPSHOT),
    KVS(ApiConsts.MASK_KVS),
    // The various DRBD Proxy configuration sections are considered separate objects for the purposes of setting
    // properties, even though the properties are stored in the resource definition
    DRBD_PROXY(0),
    DRBD_PROXY_ZSTD(0),
    DRBD_PROXY_ZLIB(0),
    DRBD_PROXY_LZMA(0),
    DRBD_PROXY_LZ4(0);

    public final long apiMask;

    LinStorObject(long apiMaskRef)
    {
        apiMask = apiMaskRef;
    }

    public static LinStorObject drbdProxyCompressionObject(String compressionType)
    {
        LinStorObject linStorObject;
        switch (compressionType)
        {
            case ApiConsts.VAL_DRBD_PROXY_COMPRESSION_ZSTD:
                linStorObject = LinStorObject.DRBD_PROXY_ZSTD;
                break;
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
