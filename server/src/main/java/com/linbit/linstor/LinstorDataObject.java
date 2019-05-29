package com.linbit.linstor;

import com.linbit.linstor.api.ApiCallRc;

public interface LinstorDataObject
{
    ApiCallRc getReports();

    void addReports(ApiCallRc apiCallRc);

    void clearReports();
}
