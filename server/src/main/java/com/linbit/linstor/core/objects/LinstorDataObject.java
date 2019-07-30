package com.linbit.linstor.core.objects;

import com.linbit.linstor.api.ApiCallRc;

public interface LinstorDataObject
{
    ApiCallRc getReports();

    void addReports(ApiCallRc apiCallRc);

    void clearReports();
}
