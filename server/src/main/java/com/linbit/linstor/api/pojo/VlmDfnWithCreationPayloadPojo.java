package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;

public class VlmDfnWithCreationPayloadPojo implements VolumeDefinitionWithCreationPayload
{
    private final VolumeDefinitionApi vlmDfnApi;
    private final Integer drbdMinorNr;

    public VlmDfnWithCreationPayloadPojo(VolumeDefinitionApi vlmDfnApiRef, Integer drbdMinorNrRef)
    {
        vlmDfnApi = vlmDfnApiRef;
        drbdMinorNr = drbdMinorNrRef;
    }

    @Override
    public VolumeDefinitionApi getVlmDfn()
    {
        return vlmDfnApi;
    }

    @Override
    public Integer getDrbdMinorNr()
    {
        return drbdMinorNr;
    }

}
