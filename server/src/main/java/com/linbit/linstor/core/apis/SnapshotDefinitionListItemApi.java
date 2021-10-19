package com.linbit.linstor.core.apis;

import java.util.List;

public interface SnapshotDefinitionListItemApi extends SnapshotDefinitionApi
{
    List<String> getNodeNames();
}
