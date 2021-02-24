package com.linbit.linstor.core.apis;

import java.util.List;
import java.util.Map;

public interface BackupListApi
{
    String getSnapKey();

    String getMetaName();

    String getFinishedTime();

    Long getFinishedTimestamp();

    String getNode();

    Boolean isShipping();

    Boolean successful();

    Map<String, String> getVlms();

    List<BackupListApi> getInc();
}
