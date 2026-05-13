package com.linbit.linstor.core;

import com.linbit.linstor.core.apicallhandler.controller.internal.SatelliteRetcodeHandler;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

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

        // Declare the multibinder so CtrlSatelliteUpdateCaller's SatelliteRetcodeDispatcher
        // dependency resolves to an empty Set in tests (no handlers registered).
        Multibinder.newSetBinder(binder(), SatelliteRetcodeHandler.class);
    }
}
