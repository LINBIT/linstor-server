package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;

import javax.annotation.Nullable;

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

    /**
     * Add this point the passphrase is already stored in the layer data, so always return null.
     */
    @Override
    public @Nullable String passphrase()
    {
        return null;
    }
}
