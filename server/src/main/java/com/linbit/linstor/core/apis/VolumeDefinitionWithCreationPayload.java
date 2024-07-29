package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

public interface VolumeDefinitionWithCreationPayload
{
    VolumeDefinitionApi getVlmDfn();
    Integer getDrbdMinorNr();

    @Nullable String passphrase();
}
