package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnWtihCreationPayload;
import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnApi;

public class VlmDfnWithCreationPayloadPojo implements VlmDfnWtihCreationPayload
{
    private final VlmDfnApi vlmDfnApi;
    private final Integer drbdMinorNr;

    public VlmDfnWithCreationPayloadPojo(VlmDfnApi vlmDfnApiRef, Integer drbdMinorNrRef)
    {
        vlmDfnApi = vlmDfnApiRef;
        drbdMinorNr = drbdMinorNrRef;
    }

    @Override
    public VlmDfnApi getVlmDfn()
    {
        return vlmDfnApi;
    }

    @Override
    public Integer getDrbdMinorNr()
    {
        return drbdMinorNr;
    }

}
