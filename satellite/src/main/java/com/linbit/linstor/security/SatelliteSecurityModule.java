package com.linbit.linstor.security;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class SatelliteSecurityModule extends AbstractModule
{
    @Override
    protected void configure()
    {
    }

    @Provides
    public SecurityLevelSetter securityLevelSetter()
    {
        return (accCtx, newLevel) ->
            SecurityLevel.set(accCtx, newLevel, null, null);
    }

    @Provides
    public MandatoryAuthSetter mandatoryAuthSetter()
    {
        return (accCtx, newPolicy) ->
            Authentication.setRequired(accCtx, newPolicy, null, null);
    }
}
