package com.linbit.linstor.core.apis;

public interface SnapshotShippingListItemApi extends SnapshotDefinitionListItemApi
{
    String getSourceNodeName();

    String getTargetNodeName();

    String getShippingStatus();
}
