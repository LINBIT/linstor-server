package com.linbit.linstor.event;

import com.google.inject.AbstractModule;
import com.linbit.linstor.event.generator.ResourceStateGenerator;
import com.linbit.linstor.event.generator.VolumeDiskStateGenerator;
import com.linbit.linstor.event.generator.controller.CtrlResourceStateGenerator;
import com.linbit.linstor.event.generator.controller.CtrlVolumeDiskStateGenerator;

public class ControllerEventModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(VolumeDiskStateGenerator.class).to(CtrlVolumeDiskStateGenerator.class);
        bind(ResourceStateGenerator.class).to(CtrlResourceStateGenerator.class);
    }
}
