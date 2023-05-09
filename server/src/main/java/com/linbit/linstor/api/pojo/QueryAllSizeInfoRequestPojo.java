package com.linbit.linstor.api.pojo;

public class QueryAllSizeInfoRequestPojo
{
    private final AutoSelectFilterPojo autoSelectFilterData;

    public QueryAllSizeInfoRequestPojo(AutoSelectFilterPojo autoSelectFilterDataRef)
    {
        autoSelectFilterData = autoSelectFilterDataRef;
    }

    public AutoSelectFilterPojo getAutoSelectFilterData()
    {
        return autoSelectFilterData;
    }
}
