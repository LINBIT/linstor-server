package com.linbit.linstor.api.pojo;

public class ExtFileStatusPojo
{
    private final String actualPath;
    private final boolean contentMatch;

    public ExtFileStatusPojo(String actualPathRef, boolean contentMatchRef)
    {
        actualPath = actualPathRef;
        contentMatch = contentMatchRef;
    }

    public String getActualPath()
    {
        return actualPath;
    }

    public boolean isContentMatch()
    {
        return contentMatch;
    }
}
