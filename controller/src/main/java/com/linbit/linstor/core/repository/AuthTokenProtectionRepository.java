package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.objects.AuthToken;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Holds the singleton auth token map protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class AuthTokenProtectionRepository implements AuthTokenRepository
{
    private final ControllerCoreModule.AuthTokenMap authTokenMap;

    @Inject
    public AuthTokenProtectionRepository(ControllerCoreModule.AuthTokenMap authTokenMapRef)
    {
        this.authTokenMap = authTokenMapRef;
    }

    @Override
    public @Nullable AuthToken get(int idRef)
    {
        return authTokenMap.get(idRef);
    }

    @Override
    public void put(int idRef, AuthToken authToken)
    {
        authTokenMap.put(idRef, authToken);
    }

    @Override
    public void remove(int idRef)
    {
        authTokenMap.remove(idRef);
    }

    @Override
    @SuppressWarnings("DescendantToken")
    public @Nullable AuthToken findByTokenHash(String tokenHash)
    {
        for (AuthToken authToken : authTokenMap.values())
        {
            if (tokenHash.equals(authToken.getTokenHash()))
            {
                return authToken;
            }
        }
        return null;
    }

    @Override
    @SuppressWarnings("DescendantToken")
    public @Nullable AuthToken findActiveSystemTokenByDescription(String description)
    {
        for (AuthToken authToken : authTokenMap.values())
        {
            if (!authToken.isUserToken() &&
                authToken.getDeletedAt() == null &&
                authToken.isActive() &&
                description.equals(authToken.getDescription()))
            {
                return authToken;
            }
        }
        return null;
    }

    @Override
    public ControllerCoreModule.AuthTokenMap getMapForView()
    {
        return authTokenMap;
    }
}
