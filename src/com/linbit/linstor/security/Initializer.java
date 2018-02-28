package com.linbit.linstor.security;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linbit.ControllerLinstorModule;
import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteLinstorModule;
import com.linbit.drbd.md.MetaDataModule;
import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.core.ApiCallHandlerModule;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.ControllerSatelliteConnectorModule;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlApiCallHandlerModule;
import com.linbit.linstor.core.LinStorArguments;
import com.linbit.linstor.core.LinStorArgumentsModule;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.core.SatelliteCoreModule;
import com.linbit.linstor.dbcp.DbConnectionPoolModule;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.SatelliteDbModule;
import com.linbit.linstor.debug.ControllerDebugModule;
import com.linbit.linstor.debug.DebugModule;
import com.linbit.linstor.debug.SatelliteDebugModule;
import com.linbit.linstor.drbdstate.DrbdStateModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.netcom.NetComModule;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.timer.CoreTimerModule;

import java.sql.SQLException;
import java.util.List;

/**
 * Initializes Controller and Satellite instances with the system's security context
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Initializer
{
    public Controller initController(
        LinStorArguments cArgs,
        ErrorReporter errorLog,
        ApiType apiType,
        List<Class<? extends ApiCall>> apiCalls
    )
    {
        Injector injector = Guice.createInjector(new GuiceConfigModule(),
            new LoggingModule(errorLog),
            new SecurityModule(),
            new ControllerSecurityModule(),
            new LinStorArgumentsModule(cArgs),
            new ConfigModule(),
            new CoreTimerModule(),
            new MetaDataModule(),
            new ControllerLinstorModule(),
            new LinStorModule(),
            new CoreModule(),
            new ControllerCoreModule(),
            new ControllerSatelliteConnectorModule(),
            new ControllerDbModule(),
            new DbConnectionPoolModule(),
            new NetComModule(),
            new NumberPoolModule(),
            new ApiModule(apiType, apiCalls),
            new ApiCallHandlerModule(),
            new CtrlApiCallHandlerModule(),
            new DebugModule(),
            new ControllerDebugModule()
        );

        return new Controller(injector);
    }

    public Satellite initSatellite(
        LinStorArguments cArgs,
        ErrorReporter errorLog,
        ApiType apiType,
        List<Class<? extends ApiCall>> apiCalls
    )
    {
        Injector injector = Guice.createInjector(new GuiceConfigModule(),
            new LoggingModule(errorLog),
            new SecurityModule(),
            new SatelliteSecurityModule(),
            new LinStorArgumentsModule(cArgs),
            new CoreTimerModule(),
            new SatelliteLinstorModule(),
            new CoreModule(),
            new SatelliteCoreModule(),
            new SatelliteDbModule(),
            new DrbdStateModule(),
            new ApiModule(apiType, apiCalls),
            new ApiCallHandlerModule(),
            new DebugModule(),
            new SatelliteDebugModule()
        );

        return new Satellite(injector);
    }

    public static void load(AccessContext accCtx, ControllerDatabase ctrlDb, DbAccessor driver)
        throws SQLException, AccessDeniedException, InvalidNameException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        SecurityLevel.load(ctrlDb, driver);
        Identity.load(ctrlDb, driver);
        SecurityType.load(ctrlDb, driver);
        Role.load(ctrlDb, driver);
    }
}
