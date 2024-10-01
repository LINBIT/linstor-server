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
import com.linbit.linstor.dbdrivers.interfaces.SecIdRoleCtrlDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdRoleMap.IDENTITY_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecIdRoleMap.ROLE_NAME;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SecIdRoleDbDriver extends AbsDatabaseDriver<Pair<Identity, Role>, Void, Void>
    implements SecIdRoleCtrlDatabaseDriver
{
    @Inject
    SecIdRoleDbDriver(@SystemContext AccessContext dbCtxRef, ErrorReporter errorReporterRef, DbEngine dbEngineRef)
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_ID_ROLE_MAP, dbEngineRef);

        setColumnSetter(IDENTITY_NAME, pair -> pair.objA.name.value);
        setColumnSetter(ROLE_NAME, pair -> pair.objB.name.value);
    }

    @Override
    protected @Nullable Pair<Pair<Identity, Role>, Void> load(RawParameters rawRef, Void parent)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        // noop, id/roles are queried on the fly if needed

        return null;
    }

    @Override
    protected String getId(Pair<Identity, Role> dataRef) throws AccessDeniedException
    {
        return "Id: " + dataRef.objA.name.displayValue + ", Role: " + dataRef.objB.name.displayValue;
    }
}
