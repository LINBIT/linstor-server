package com.linbit.linstor.api.pojo;

public class QuerySizeInfoRequestPojo
{

    private final String rscGrpName;
    private final AutoSelectFilterPojo autoSelectFilterData;

    public QuerySizeInfoRequestPojo(String rscGrpNameRef, AutoSelectFilterPojo autoSelectFilterDataRef)
    {
        rscGrpName = rscGrpNameRef;
        autoSelectFilterData = autoSelectFilterDataRef;
    }

    public AutoSelectFilterPojo getAutoSelectFilterData()
    {
        return autoSelectFilterData;
    }

    public String getRscGrpName()
    {
        return rscGrpName;
    }
}
