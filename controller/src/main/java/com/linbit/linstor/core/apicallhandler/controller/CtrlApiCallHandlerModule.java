package com.linbit.linstor.core.apicallhandler.controller;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class CtrlApiCallHandlerModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        Multibinder<CtrlSatelliteConnectionListener> commandsBinder =
            Multibinder.newSetBinder(binder(), CtrlSatelliteConnectionListener.class);

        commandsBinder.addBinding().to(CtrlNodeDeleteApiCallHandler.class);
        commandsBinder.addBinding().to(CtrlRscDeleteApiCallHandler.class);
        commandsBinder.addBinding().to(CtrlRscDfnDeleteApiCallHandler.class);
        commandsBinder.addBinding().to(CtrlRscToggleDiskApiCallHandler.class);
        commandsBinder.addBinding().to(CtrlVlmDfnDeleteApiCallHandler.class);
        commandsBinder.addBinding().to(CtrlVlmDfnModifyApiCallHandler.class);
        commandsBinder.addBinding().to(CtrlSnapshotDeleteApiCallHandler.class);
        commandsBinder.addBinding().to(CtrlSnapshotRollbackApiCallHandler.class);
    }
}
