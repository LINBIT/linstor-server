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
import com.linbit.linstor.dbdrivers.interfaces.SecConfigCtrlDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecConfigDatabaseDriver.SecConfigDbEntry;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecConfiguration.ENTRY_DSP_KEY;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecConfiguration.ENTRY_KEY;
import static com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.SecConfiguration.ENTRY_VALUE;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class SecConfigDbDriver extends AbsDatabaseDriver<SecConfigDbEntry, Void, Void>
    implements SecConfigCtrlDatabaseDriver
{
    private final SingleColumnDatabaseDriver<SecConfigDbEntry, String> valueDriver;

    @Inject
    public SecConfigDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.SEC_CONFIGURATION, dbEngineRef);

        setColumnSetter(
            ENTRY_KEY,
            secCfgEntry -> secCfgEntry.key.toUpperCase()
        );
        setColumnSetter(ENTRY_DSP_KEY, secCfgEntry -> secCfgEntry.key);
        setColumnSetter(ENTRY_VALUE, secCfgEntry -> secCfgEntry.value);

        valueDriver = generateSingleColumnDriver(
            ENTRY_VALUE,
            secCfgEntry -> secCfgEntry.value,
            Function.identity()
        );
    }

    @Override
    public SingleColumnDatabaseDriver<SecConfigDbEntry, String> getValueDriver()
    {
        return valueDriver;
    }

    @Override
    protected Pair<SecConfigDbEntry, Void> load(RawParameters rawRef, Void parentRef)
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException
    {
        return new Pair<>(
            new SecConfigDbEntry(
                rawRef.get(ENTRY_DSP_KEY),
                rawRef.get(ENTRY_VALUE)
            ),
            null
        );
    }

    @Override
    protected String getId(SecConfigDbEntry dataRef) throws AccessDeniedException
    {
        return "SecurityConfiguration key: " + dataRef.key + ", Value: " + dataRef.value;
    }
}
