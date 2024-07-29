package com.linbit.linstor.security;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.SecDefaultRoleCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecDfltRoles.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecDfltRoles.ROLE_NAME;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SecDefaultRoleDbDriver extends AbsDatabaseDriver<Pair<Identity, Role>, Void, Void>
    implements SecDefaultRoleCtrlDatabaseDriver
{
    @Inject
    public SecDefaultRoleDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_DFLT_ROLES, dbEngineRef, null);

        setColumnSetter(IDENTITY_NAME, pair -> pair.objA.name.value);
        setColumnSetter(ROLE_NAME, pair -> pair.objB.name.value);
    }

    @Override
    protected @Nullable Pair<Pair<Identity, Role>, Void> load(RawParameters rawRef, Void parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        // noop, id/roles are queried on the fly if needed

        return null;
    }

    @Override
    protected String getId(Pair<Identity, Role> dataRef) throws AccessDeniedException
    {
        return "Default role " + dataRef.objB.name.displayValue + " for identity: " + dataRef.objA.name.displayValue;
    }
}
