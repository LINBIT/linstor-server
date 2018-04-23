package com.linbit.linstor.event;

import com.google.inject.AbstractModule;
import com.linbit.linstor.event.generator.VolumeDiskStateGenerator;
import com.linbit.linstor.event.generator.controller.CtrlVolumeDiskStateGenerator;

public class ControllerEventModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(VolumeDiskStateGenerator.class).to(CtrlVolumeDiskStateGenerator.class);
    }
}
