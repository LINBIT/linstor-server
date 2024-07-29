package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

public interface SatelliteConfigApi
{
    @Nullable
    String getLogLevel();

    @Nullable
    String getLogLevelLinstor();
}
