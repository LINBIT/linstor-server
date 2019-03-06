package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.requests.MsgCrtVlmDfnOuterClass.VlmDfnWithPayload;

public class VlmDfnWithPayloadApiData implements VolumeDefinition.VlmDfnWtihCreationPayload
{
    private final VlmDfn vlmDfn;
    private final Integer minor;

    public VlmDfnWithPayloadApiData(VlmDfnWithPayload vlmDfnWithPayloadRef)
    {
        vlmDfn = vlmDfnWithPayloadRef.getVlmDfn();
        minor = vlmDfnWithPayloadRef.hasDrbdMinorNr() ? vlmDfnWithPayloadRef.getDrbdMinorNr() : null;
    }

    public VlmDfnWithPayloadApiData(VlmDfn vlmDfnRef)
    {
        vlmDfn = vlmDfnRef;
        minor = null;
    }

    @Override
    public VlmDfnApi getVlmDfn()
    {
        return new VlmDfnApiData(vlmDfn);
    }

    @Override
    public Integer getDrbdMinorNr()
    {
        return minor;
    }

}
