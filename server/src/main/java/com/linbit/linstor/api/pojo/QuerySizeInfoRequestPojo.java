package com.linbit.linstor.api.pojo;

public class QuerySizeInfoRequestPojo
{
    private final String rscGrpName;
    private final AutoSelectFilterPojo autoSelectFilterData;
    private final int ignoreCacheOlderThanSec;

    public QuerySizeInfoRequestPojo(
        String rscGrpNameRef,
        AutoSelectFilterPojo autoSelectFilterDataRef,
        int ignoreCacheOlderThanSecRef
    )
    {
        rscGrpName = rscGrpNameRef;
        autoSelectFilterData = autoSelectFilterDataRef;
        ignoreCacheOlderThanSec = ignoreCacheOlderThanSecRef;
    }

    public AutoSelectFilterPojo getAutoSelectFilterData()
    {
        return autoSelectFilterData;
    }

    public String getRscGrpName()
    {
        return rscGrpName;
    }

    public int getIgnoreCacheOlderThanSec()
    {
        return ignoreCacheOlderThanSec;
    }
}
