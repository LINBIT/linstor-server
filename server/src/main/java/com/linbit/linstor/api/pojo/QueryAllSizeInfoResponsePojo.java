package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.ApiCallRc;

import java.util.Map;

public class QueryAllSizeInfoResponsePojo
{
    private final Map<String/* RscGrpName */, QueryAllSizeInfoResponseEntryPojo> map;
    private final ApiCallRc apiCallRc;

    public QueryAllSizeInfoResponsePojo(
        Map<String /* RscGrpName */, QueryAllSizeInfoResponseEntryPojo> mapRef,
        ApiCallRc apiCallRcRef
    )
    {
        map = mapRef;
        apiCallRc = apiCallRcRef;
    }

    public Map<String /* RscGrpName */, QueryAllSizeInfoResponseEntryPojo> getResult()
    {
        return map;
    }

    public ApiCallRc getApiCallRc()
    {
        return apiCallRc;
    }

    public static class QueryAllSizeInfoResponseEntryPojo
    {
        private final QuerySizeInfoResponsePojo qsiRespPojo;
        private final ApiCallRc apiCallRc;

        public QueryAllSizeInfoResponseEntryPojo(QuerySizeInfoResponsePojo qsiRespPojoRef, ApiCallRc apiCallRcRef)
        {
            qsiRespPojo = qsiRespPojoRef;
            apiCallRc = apiCallRcRef;
        }

        public QuerySizeInfoResponsePojo getQsiRespPojo()
        {
            return qsiRespPojo;
        }

        public ApiCallRc getApiCallRc()
        {
            return apiCallRc;
        }
    }
}
