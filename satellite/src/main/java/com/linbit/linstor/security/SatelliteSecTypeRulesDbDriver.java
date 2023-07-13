package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesDatabaseDriver;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecTypeRulesDbDriver
    extends AbsSatelliteDbDriver<TypeEnforcementRulePojo>
    implements SecTypeRulesDatabaseDriver
{
    @Inject
    public SatelliteSecTypeRulesDbDriver()
    {
        // no-op
    }
}
