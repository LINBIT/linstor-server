package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;

public class VlmDfnWithCreationPayloadPojo implements VolumeDefinitionWithCreationPayload
{
    private final VolumeDefinitionApi vlmDfnApi;
    private final @Nullable Integer drbdMinorNr;

    public VlmDfnWithCreationPayloadPojo(VolumeDefinitionApi vlmDfnApiRef, @Nullable Integer drbdMinorNrRef)
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
    public @Nullable Integer getDrbdMinorNr()
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
