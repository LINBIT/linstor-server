package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.objects.AuthToken;

/**
 * Provides access to auth tokens with automatic security checks.
 */
public interface AuthTokenRepository
{
    @Nullable AuthToken get(int idRef);

    void put(int idRef, AuthToken authToken);

    void remove(int idRef);

    @Nullable AuthToken findByTokenHash(String tokenHash);

    @Nullable AuthToken findActiveSystemTokenByDescription(String description);

    ControllerCoreModule.AuthTokenMap getMapForView();
}
