package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWtihCreationPayload;

public class VlmDfnWithCreationPayloadPojo implements VolumeDefinitionWtihCreationPayload
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
