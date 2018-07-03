package com.linbit.linstor.api.interfaces;

import java.util.List;

public interface AutoSelectFilterApi
{
    int getPlaceCount();

    String getStorPoolNameStr();

    List<String> getNotPlaceWithRscList();

    String getNotPlaceWithRscRegex();

    List<String> getReplicasOnSameList();

    List<String> getReplicasOnDifferentList();
}
