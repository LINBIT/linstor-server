package com.linbit.linstor.event.writer.protobuf.common;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.generator.ResourceStateGenerator;
import com.linbit.linstor.event.generator.VolumeDiskStateGenerator;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventWriter(
    eventName = ApiConsts.EVENT_RESOURCE_STATE,
    objectType = WatchableObject.RESOURCE
)
@Singleton
public class ResourceStateEvent implements EventWriter
{
    private final CommonSerializer commonSerializer;
    private final ResourceStateGenerator resourceStateGenerator;

    @Inject
    public ResourceStateEvent(
        CommonSerializer commonSerializerRef,
        ResourceStateGenerator resourceStateGeneratorRef
    )
    {
        commonSerializer = commonSerializerRef;
        resourceStateGenerator = resourceStateGeneratorRef;
    }

    @Override
    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        Boolean resourceReady = resourceStateGenerator.generate(objectIdentifier);

        return resourceReady == null ? null : commonSerializer.builder().resourceStateEvent(resourceReady).build();
    }
}
