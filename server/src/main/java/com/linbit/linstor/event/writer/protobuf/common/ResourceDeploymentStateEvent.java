package com.linbit.linstor.event.writer.protobuf.common;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.generator.ResourceDeploymentStateGenerator;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventWriter(
    eventName = ApiConsts.EVENT_RESOURCE_DEPLOYMENT_STATE,
    objectType = WatchableObject.RESOURCE
)
@Singleton
public class ResourceDeploymentStateEvent implements EventWriter
{
    private final CommonSerializer commonSerializer;
    private final ResourceDeploymentStateGenerator resourceDeploymentStateGenerator;

    @Inject
    public ResourceDeploymentStateEvent(
        CommonSerializer commonSerializerRef,
        ResourceDeploymentStateGenerator resourceDeploymentStateGeneratorRef
    )
    {
        commonSerializer = commonSerializerRef;
        resourceDeploymentStateGenerator = resourceDeploymentStateGeneratorRef;
    }

    @Override
    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        ApiCallRc apiCallRc = resourceDeploymentStateGenerator.generate(objectIdentifier);

        return apiCallRc == null ? null :
            commonSerializer.headerlessBuilder().resourceDeploymentStateEvent(apiCallRc).build();
    }

    @Override
    public void clear(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        resourceDeploymentStateGenerator.clear(objectIdentifier);
    }
}
