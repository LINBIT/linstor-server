package com.linbit.linstor.core.apis;

import com.linbit.linstor.annotation.Nullable;

public interface ControllerConfigApi
{
    @Nullable
    String getLogLevel();

    @Nullable
    String getLogLevelLinstor();

    @Nullable
    String getLogLevelGlobal();

    @Nullable
    String getLogLevelLinstorGlobal();
}
