package com.linbit.linstor.api.pojo;

public class QueryAllSizeInfoRequestPojo
{
    private final AutoSelectFilterPojo autoSelectFilterData;
    private final int ignoreCacheOlderThanSec;

    public QueryAllSizeInfoRequestPojo(AutoSelectFilterPojo autoSelectFilterDataRef, int ignoreCacheOlderThanSecRef)
    {
        autoSelectFilterData = autoSelectFilterDataRef;
        ignoreCacheOlderThanSec = ignoreCacheOlderThanSecRef;
    }

    public AutoSelectFilterPojo getAutoSelectFilterData()
    {
        return autoSelectFilterData;
    }

    public int getIgnoreCacheOlderThanSec()
    {
        return ignoreCacheOlderThanSec;
    }
}
