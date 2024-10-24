package com.linbit.linstor.api.prop;

import com.linbit.linstor.api.ApiConsts;

public enum LinStorObject
{
    // ctrl- & stlt-path do not need trailing slashes, because unlike all the others they are not prefixes
    // but the full path
    CTRL(ApiConsts.MASK_CTRL_CONF, "/CTRLCFG"),
    STLT(ApiConsts.MASK_CTRL_CONF, "STLTCFG"), // TODO: migration for adding leading slash

    NODE(ApiConsts.MASK_NODE, "/NODES/"),
    NODE_CONN(ApiConsts.MASK_NODE_CONN, "/CONDFN/NODES/"),
    RSC_GRP(ApiConsts.MASK_RSC_GRP, "/RESOURCEGROUPS/"),
    RSC_DFN(ApiConsts.MASK_RSC_DFN, "/RESOURCEDEFINITIONS/"),
    RSC(ApiConsts.MASK_RSC, "/RESOURCES/"),
    RSC_CONN(ApiConsts.MASK_RSC_CONN, "/CONDFN/RESOURCES/"),
    VLM_GRP(ApiConsts.MASK_VLM_GRP, "/VOLUMEGROUPS/"),
    VLM_DFN(ApiConsts.MASK_VLM_DFN, "/VOLUMEDEFINITIONS/"),
    VLM(ApiConsts.MASK_VLM, "/VOLUMES/"),
    VLM_CONN(ApiConsts.MASK_VLM_CONN, "/CONDFN/VOLUME/"),
    STOR_POOL(ApiConsts.MASK_STOR_POOL, "/STORPOOLCONF/"),
    STOR_POOL_DFN(ApiConsts.MASK_STOR_POOL_DFN, "/STORPOOLDFNCONF/"),
    SNAP_DFN(0, "/SNAPSHOTDEFINITIONS/"),
    SNAP(ApiConsts.MASK_SNAPSHOT, "/SNAPSHOTS/"),
    SNAP_VLM(ApiConsts.MASK_SNAPSHOT, "/SNAPSHOTS/"),
    SNAP_VLM_DFN(0, "/SNAPSHOTVOLUMEDEFINITIONS/"),
    KVS(ApiConsts.MASK_KVS, "/KEYVALUESTORES/"),

    // the following objects do not have their own props-containers and therefore need no props-path
    NET_IF(ApiConsts.MASK_NET_IF, null),
    // The various DRBD Proxy configuration sections are considered separate objects for the purposes of setting
    // properties, even though the properties are stored in the resource definition
    DRBD_PROXY(0, null),
    DRBD_PROXY_ZSTD(0, null),
    DRBD_PROXY_ZLIB(0, null),
    DRBD_PROXY_LZMA(0, null),
    DRBD_PROXY_LZ4(0, null);

    public final long apiMask;
    public final String path;

    LinStorObject(long apiMaskRef, String pathRef)
    {
        apiMask = apiMaskRef;
        path = pathRef;
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
