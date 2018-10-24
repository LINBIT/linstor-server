package com.linbit.linstor.api.prop;

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
    // DRBD Proxy is considered a separate object for the purposes of setting properties, even though the properties
    // are stored in the resource definition
    DRBD_PROXY;

    LinStorObject()
    {
    }
}
