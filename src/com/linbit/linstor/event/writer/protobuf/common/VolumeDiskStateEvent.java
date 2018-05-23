package com.linbit.linstor.event.writer.protobuf.common;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.generator.VolumeDiskStateGenerator;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventWriter(
    eventName = ApiConsts.EVENT_VOLUME_DISK_STATE,
    objectType = WatchableObject.VOLUME
)
@Singleton
public class VolumeDiskStateEvent implements EventWriter
{
    private final CommonSerializer commonSerializer;
    private final VolumeDiskStateGenerator volumeDiskStateGenerator;

    @Inject
    public VolumeDiskStateEvent(
        CommonSerializer commonSerializerRef,
        VolumeDiskStateGenerator volumeDiskStateGeneratorRef
    )
    {
        commonSerializer = commonSerializerRef;
        volumeDiskStateGenerator = volumeDiskStateGeneratorRef;
    }

    @Override
    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        String diskState = volumeDiskStateGenerator.generate(objectIdentifier);

        return diskState == null ? null : commonSerializer.builder().volumeDiskState(diskState).build();
    }
}
