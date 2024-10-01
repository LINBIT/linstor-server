package com.linbit.linstor.security;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
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
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver.SecRoleInit;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleCtrlDatabaseDriver.SecRoleParent;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles.DOMAIN_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles.ROLE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles.ROLE_ENABLED;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles.ROLE_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecRoles.ROLE_PRIVILEGES;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class SecRoleDbDriver extends AbsDatabaseDriver<Role, SecRoleInit, SecRoleParent>
    implements SecRoleCtrlDatabaseDriver
{
    private static final String DFLT_DOMAIN_NAME = "PUBLIC";

    private final AccessContext dbCtx;
    private final SingleColumnDatabaseDriver<Role, SecurityType> domainDriver;
    private final SingleColumnDatabaseDriver<Role, Boolean> roleEnabledDriver;

    @Inject
    public SecRoleDbDriver(@SystemContext AccessContext dbCtxRef, ErrorReporter errorReporterRef, DbEngine dbEngineRef)
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_ROLES, dbEngineRef);
        dbCtx = dbCtxRef;

        setColumnSetter(ROLE_NAME, role -> role.name.value);
        setColumnSetter(ROLE_DSP_NAME, role -> role.name.displayValue);

        setColumnSetter(DOMAIN_NAME, ignored -> DFLT_DOMAIN_NAME);
        // by default new roles have 0 privileges
        setColumnSetter(ROLE_PRIVILEGES, ignored -> 0);
        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                // by default new roles are always enabled
                setColumnSetter(ROLE_ENABLED, ignored -> true);

                roleEnabledDriver = generateSingleColumnDriver(
                    ROLE_ENABLED,
                    this::getId,
                    Function.identity()
                );
                break;
            case ETCD:
                // by default new roles are always enabled
                setColumnSetter(ROLE_ENABLED, ignored -> Boolean.TRUE.toString());

                roleEnabledDriver = generateSingleColumnDriver(
                    ROLE_ENABLED,
                    this::getId,
                    enabled -> Boolean.toString(enabled)
                );
                break;
            default:
                throw new ImplementationError("Unexpected Db type: " + getDbType());
        }
        domainDriver = generateSingleColumnDriver(
            DOMAIN_NAME,
            role -> role.name.displayValue,
            secType -> secType.name.value
        );
    }

    @Override
    public SingleColumnDatabaseDriver<Role, SecurityType> getDomainDriver()
    {
        return domainDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Role, Boolean> getRoleEnabledDriver()
    {
        return roleEnabledDriver;
    }

    @Override
    protected Pair<Role, SecRoleInit> load(RawParameters rawRef, SecRoleParent parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        final Privilege[] limit = Privilege.restore(rawRef.getParsed(ROLE_PRIVILEGES));
        final Role role = Role.create(
            dbCtx,
            rawRef.build(ROLE_DSP_NAME, RoleName::new),
            new PrivilegeSet(limit)
        );

        final boolean roleEnabled = rawRef.getParsed(ROLE_ENABLED);

        return new Pair<>(
            role,
            new SecRoleInit(
                roleEnabled,
                parentRef.getSecType(rawRef.build(DOMAIN_NAME, SecTypeName::new))
            )
        );
    }

    @Override
    protected String getId(Role dataRef) throws AccessDeniedException
    {
        return "Role " + dataRef.name.displayValue;
    }
}
