package com.linbit.linstor.core;

import com.linbit.GuiceConfigModule;
import com.linbit.SystemServiceStartException;
import com.linbit.drbd.md.MetaDataModule;
import com.linbit.linstor.ControllerLinstorModule;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.ApiType;
import com.linbit.linstor.api.BaseApiCall;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.LinStorScope.ScopeAutoCloseable;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiType;
import com.linbit.linstor.core.apicallhandler.ApiCallHandlerModule;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandlerModule;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportImportHelper;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.CtrlConfigModule;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.debug.ControllerDebugModule;
import com.linbit.linstor.debug.DebugModule;
import com.linbit.linstor.event.EventModule;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.ConnectionStateEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.DonePercentageEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.ReplicationStateEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.ResourceStateEventHandler;
import com.linbit.linstor.event.handler.protobuf.controller.VolumeDiskStateEventHandler;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.ConnectionStateEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.DonePercentageEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.ReplicationStateEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.ResourceStateEventSerializer;
import com.linbit.linstor.event.serializer.protobuf.common.VolumeDiskStateEventSerializer;
import com.linbit.linstor.layer.LayerSizeCalculatorModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.netcom.NetComModule;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.SecurityModule;
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.ControllerTransactionMgrModule;
import com.linbit.linstor.utils.NameShortenerModule;
import com.linbit.utils.InjectorLoader;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import picocli.CommandLine;

public class LinstorDatabaseTool
{
    private static CommandLine commandLine;

    @CommandLine.Command(
        name = "linstor-database",
        subcommands =
        {
            CmdExportDb.class,
            CmdImportDb.class,
    })
    private static class LinstorConfigCmd implements Callable<Object>
    {
        @Override
        public Object call()
        {
            commandLine.usage(System.err);
            return null;
        }
    }

    @CommandLine.Command(
        name = "export-db",
        description = "Exports the given database to a given file"
    )
    private static class CmdExportDb implements Callable<Object>
    {
        @CommandLine.Option(names = {"-c", "--config-directory"},
            description = "Configuration directory for the controller"
        )
        private String configurationDirectory = "/etc/linstor";

        @CommandLine.Parameters(description = "Path to the exported database file")
        private @Nullable String dbExportPath;

        @Override
        public Object call() throws Exception
        {
            runDbExportImport(configurationDirectory, injector ->
            {
                DbExportImportHelper dbExportImporter = injector.getInstance(DbExportImportHelper.class);
                ErrorReporter errorLog = injector.getInstance(ErrorReporter.class);
                dbExportImporter.export(dbExportPath);
                errorLog.logInfo("Export finished");
            });

            return null;
        }
    }

    @CommandLine.Command(
        name = "import-db",
        description = "Imports a previously exported linstor database-dump to the database" +
            "given by the configured linstor.toml"
    )
    private static class CmdImportDb implements Callable<Object>
    {
        @CommandLine.Option(names = { "-c", "--config-directory" },
            description = "Configuration directory for the controller"
        )
        private String configurationDirectory = "/etc/linstor";

        @CommandLine.Parameters(description = "Path to the exported database file")
        private @Nullable String dbExportPath;

        @Override
        public Object call() throws Exception
        {
            runDbExportImport(configurationDirectory, injector ->
            {
                DbExportImportHelper dbExportImporter = injector.getInstance(DbExportImportHelper.class);
                ErrorReporter errorLog = injector.getInstance(ErrorReporter.class);
                dbExportImporter.importDb(dbExportPath);
                errorLog.logInfo("Import finished");
            });

            return null;
        }
    }

    private static void runDbExportImport(String cfgPath, Consumer<Injector> injectorConsumer)
        throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException,
        SystemServiceStartException, InitializationException
    {
        CtrlConfig cfg = new CtrlConfig(
            new String[]
            {
                "-c",
                cfgPath
            }
        );

        ErrorReporter errorLog = new StdErrorReporter(
            "linstor-db",
            Paths.get(cfg.getLogDirectory()),
            cfg.isLogPrintStackTrace(),
            "",
            cfg.getLogLevel(),
            cfg.getLogLevelLinstor(),
            () -> null
        );

        DatabaseDriverInfo.DatabaseType dbType = Controller.checkDatabaseConfig(errorLog, cfg);
        ApiType apiType = new ProtobufApiType();
        ClassPathLoader classPathLoader = new ClassPathLoader(errorLog);

        List<String> packageSuffixes = Arrays.asList("common", "controller", "internal");

        List<Class<? extends BaseApiCall>> apiCalls = classPathLoader.loadClasses(
            ProtobufApiType.class.getPackage().getName(),
            packageSuffixes,
            BaseApiCall.class,
            ProtobufApiCall.class
        );
        List<Class<? extends EventSerializer>> eventSerializers = Arrays.asList(
            ResourceStateEventSerializer.class,
            VolumeDiskStateEventSerializer.class,
            ReplicationStateEventSerializer.class,
            DonePercentageEventSerializer.class,
            ConnectionStateEventSerializer.class
        );
        List<Class<? extends EventHandler>> eventHandlers = Arrays.asList(
            ResourceStateEventHandler.class,
            VolumeDiskStateEventHandler.class,
            ReplicationStateEventHandler.class,
            DonePercentageEventHandler.class,
            ConnectionStateEventHandler.class
        );
        final List<Module> injModList = new LinkedList<>(
            Arrays.asList(
                new GuiceConfigModule(),
                new LoggingModule(errorLog),
                new SecurityModule(),
                new ControllerSecurityModule(),
                new CtrlConfigModule(cfg),
                new CoreTimerModule(),
                new MetaDataModule(),
                new ControllerLinstorModule(),
                new LinStorModule(),
                new CoreModule(),
                new ControllerCoreModule(),
                new ControllerSatelliteCommunicationModule(),
                new ControllerDbModule(dbType),
                new NetComModule(),
                new NumberPoolModule(),
                new ApiModule(apiType, apiCalls),
                new ApiCallHandlerModule(),
                new CtrlApiCallHandlerModule(),
                new EventModule(eventSerializers, eventHandlers),
                new DebugModule(),
                new ControllerDebugModule(),
                new ControllerTransactionMgrModule(dbType),
                new NameShortenerModule(),
                new LayerSizeCalculatorModule()
            )
        );
        final boolean haveFipsInit = LinStor.initializeFips(errorLog);

        LinStor.loadModularCrypto(injModList, errorLog, haveFipsInit);
        InjectorLoader.dynLoadInjModule(Controller.SPC_TRK_MODULE_NAME, injModList, errorLog, dbType);

        final Injector injector = Guice.createInjector(injModList);

        LinStorScope scope = injector.getInstance(LinStorScope.class);
        DbInitializer dbInit = injector.getInstance(DbInitializer.class);

        ModularCryptoProvider cryptoProvider = injector.getInstance(ModularCryptoProvider.class);
        AbsMigration.setModularCryptoProvider(cryptoProvider);

        try (ScopeAutoCloseable closableScope = scope.enter())
        {
            dbInit.setEnableMigrationOnInit(false);
            dbInit.initialize();

            injectorConsumer.accept(injector);
            dbInit.shutdown(true); // make sure that databases like H2 get a chance to properly shutdown...
        }
    }

    public static void main(String[] args)
    {
        commandLine = new CommandLine(new LinstorConfigCmd());

        commandLine.parseWithHandler(new CommandLine.RunLast(), args);
    }
}
