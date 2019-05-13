package com.linbit.linstor.core;

import com.google.inject.AbstractModule;

public class ApiTestModule extends AbstractModule
{
    private SatelliteConnectorImpl mockedSatelliteConnector;

    public ApiTestModule(SatelliteConnectorImpl mockedSatelliteConnectorRef)
    {
        mockedSatelliteConnector = mockedSatelliteConnectorRef;
    }

    @Override
    protected void configure()
    {
        bind(SatelliteConnector.class).toInstance(mockedSatelliteConnector);
        bind(SatelliteConnectorImpl.class).toInstance(mockedSatelliteConnector);
    }
}
