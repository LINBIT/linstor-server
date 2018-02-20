package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlClientSerializer;

public class CtrlApiCallHandlerModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(CtrlClientSerializer.class).to(ProtoCtrlClientSerializer.class);
    }
}
