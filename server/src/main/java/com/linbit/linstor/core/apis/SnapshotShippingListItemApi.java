package com.linbit.linstor.core.apis;

@Deprecated(forRemoval = true)
public interface SnapshotShippingListItemApi extends SnapshotDefinitionListItemApi
{
    String getSourceNodeName();

    String getTargetNodeName();

    String getShippingStatus();
}
