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
import com.linbit.linstor.dbdrivers.interfaces.SecTypeCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeCtrlDatabaseDriver.SecTypeInitObj;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypes.TYPE_DSP_NAME;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypes.TYPE_ENABLED;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecTypes.TYPE_NAME;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class SecTypeDbDriver extends AbsDatabaseDriver<SecurityType, SecTypeInitObj, Void>
    implements SecTypeCtrlDatabaseDriver
{
    private final AccessContext dbCtx;
    private final SingleColumnDatabaseDriver<SecurityType, Boolean> typeEnabledDriver;

    @Inject
    public SecTypeDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporter,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporter, GeneratedDatabaseTables.SEC_TYPES, dbEngineRef);
        dbCtx = dbCtxRef;

        setColumnSetter(TYPE_NAME, type -> type.name.value);
        setColumnSetter(TYPE_DSP_NAME, type -> type.name.displayValue);
        // new entries are always enabled
        setColumnSetter(TYPE_ENABLED, ignored -> true);

        switch (getDbType())
        {
            case SQL: // fall-through
            case K8S_CRD:
                typeEnabledDriver = generateSingleColumnDriver(
                    TYPE_ENABLED,
                    this::getId,
                    Function.identity()
                );
                break;
            default:
                throw new ImplementationError("Unexpected Db type: " + getDbType());
        }
    }

    @Override
    public SingleColumnDatabaseDriver<SecurityType, Boolean> getTypeEnabledDriver()
    {
        return typeEnabledDriver;
    }

    @Override
    protected Pair<SecurityType, SecTypeInitObj> load(RawParameters rawRef, Void parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        final SecTypeName secTypeName = rawRef.build(TYPE_DSP_NAME, SecTypeName::new);
        final boolean enabled = rawRef.get(TYPE_ENABLED);

        return new Pair<>(
            SecurityType.create(dbCtx, secTypeName),
            new SecTypeInitObj(enabled)
        );
    }

    @Override
    protected String getId(SecurityType dataRef) throws AccessDeniedException
    {
        return "SecurityType: " + dataRef.name.displayValue;
    }
}
