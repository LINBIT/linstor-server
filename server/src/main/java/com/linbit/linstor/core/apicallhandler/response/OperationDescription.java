package com.linbit.linstor.core.apicallhandler.response;

public class OperationDescription
{
    private final String noun;

    private final String progressive;

    public OperationDescription(String nounRef, String progressiveRef)
    {
        noun = nounRef;
        progressive = progressiveRef;
    }

    public String getNoun()
    {
        return noun;
    }

    public String getProgressive()
    {
        return progressive;
    }
}
