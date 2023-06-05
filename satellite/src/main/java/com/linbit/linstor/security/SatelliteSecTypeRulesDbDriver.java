package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesDatabaseDriver;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecTypeRulesDbDriver implements SecTypeRulesDatabaseDriver
{
    @Inject
    public SatelliteSecTypeRulesDbDriver()
    {
    }

    @Override
    public void create(TypeEnforcementRulePojo dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(TypeEnforcementRulePojo dataRef) throws DatabaseException
    {
        // noop
    }
}
