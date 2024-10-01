package com.linbit.linstor.security;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeRulesCtrlDatabaseDriver.SecTypeRulesParent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypeRules.ACCESS_TYPE;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypeRules.DOMAIN_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypeRules.TYPE_NAME;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SecTypeRulesDbDriver extends AbsDatabaseDriver<TypeEnforcementRulePojo, Void, SecTypeRulesParent>
    implements SecTypeRulesCtrlDatabaseDriver
{
    @Inject
    public SecTypeRulesDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_TYPE_RULES, dbEngineRef);

        setColumnSetter(TYPE_NAME, teRule -> teRule.getTypeName());
        setColumnSetter(DOMAIN_NAME, teRule -> teRule.getDomainName());
        setColumnSetter(ACCESS_TYPE, teRule -> teRule.getAccessType());
    }

    @Override
    protected Pair<TypeEnforcementRulePojo, Void> load(RawParameters rawRef, SecTypeRulesParent parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        return new Pair<>(
            new TypeEnforcementRulePojo(
                rawRef.get(DOMAIN_NAME),
                rawRef.get(TYPE_NAME),
                rawRef.getParsed(ACCESS_TYPE)
            ),
            null
        );
    }

    @Override
    protected String getId(TypeEnforcementRulePojo dataRef) throws AccessDeniedException
    {
        return "TypeEnforementRule for domain: " + dataRef.getDomainName() + ", type: " + dataRef.getTypeName() +
            ", access: " + dataRef.getAccessType();
    }
}
