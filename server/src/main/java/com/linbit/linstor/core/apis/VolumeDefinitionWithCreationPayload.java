package com.linbit.linstor.core.apis;

import javax.annotation.Nullable;

public interface VolumeDefinitionWithCreationPayload
{
    VolumeDefinitionApi getVlmDfn();
    Integer getDrbdMinorNr();

    @Nullable String passphrase();
}
