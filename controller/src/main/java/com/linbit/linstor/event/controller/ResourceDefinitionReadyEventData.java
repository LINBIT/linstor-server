package com.linbit.linstor.event.controller;

public class ResourceDefinitionReadyEventData
{
    private final int readyCount;
    private final int errorCount;

    public ResourceDefinitionReadyEventData(int readyCountRef, int errorCountRef)
    {
        readyCount = readyCountRef;
        errorCount = errorCountRef;
    }

    public int getReadyCount()
    {
        return readyCount;
    }

    public int getErrorCount()
    {
        return errorCount;
    }
}
