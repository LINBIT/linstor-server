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
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportFileUtils;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportImportHelper;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.CtrlConfigModule;
import com.linbit.linstor.dbcp.DbInitializer;
import com.linbit.linstor.dbcp.migration.AbsMigration;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.H2FormatUtils;
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
import com.linbit.linstor.timer.CoreTimerModule;
import com.linbit.linstor.transaction.ControllerTransactionMgrModule;
import com.linbit.linstor.utils.NameShortenerModule;
import com.linbit.utils.InjectorLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import picocli.CommandLine;

public class LinstorDatabaseTool
{
    private static final int EXIT_CODE_MIGRATION_PENDING = 1;

    private static CommandLine commandLine;

    @CommandLine.Command(
        name = "linstor-database",
        subcommands =
        {
            CmdExportDb.class,
            CmdImportDb.class,
            CmdMigrateH2.class,
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

        @CommandLine.Option(names = {"-l", "--logs"},
            description = "Path to the log directory"
        )
        private @Nullable String logDirectory = null;

        @CommandLine.Option(names = {"--migrate-before-export"},
            description = "Run pending database migrations before exporting"
        )
        private boolean migrateBeforeExport = false;

        @CommandLine.Parameters(
            description = "Path to the exported database file. The export is gzip compressed if the file name ends " +
                "with '" + DbExportFileUtils.GZIP_SUFFIX + "'"
        )
        private @Nullable String dbExportPath;

        @Override
        public Object call() throws Exception
        {
            runDbExportImport(configurationDirectory, logDirectory, migrateBeforeExport, true, injector ->
            {
                DbExportImportHelper dbExportImporter = injector.getInstance(DbExportImportHelper.class);
                ErrorReporter errorLog = injector.getInstance(ErrorReporter.class);
                dbExportImporter.exportTo(Paths.get(dbExportPath));
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

        @CommandLine.Option(names = {"-l", "--logs"},
            description = "Path to the log directory"
        )
        private @Nullable String logDirectory = null;

        @CommandLine.Parameters(
            description = "Path to the exported database file, plain or gzip compressed (detected automatically)"
        )
        private @Nullable String dbExportPath;

        @Override
        public Object call() throws Exception
        {
            runDbExportImport(configurationDirectory, logDirectory, false, false, injector ->
            {
                DbExportImportHelper dbExportImporter = injector.getInstance(DbExportImportHelper.class);
                ErrorReporter errorLog = injector.getInstance(ErrorReporter.class);
                dbExportImporter.importDb(dbExportPath);
                errorLog.logInfo("Import finished");
            });

            return null;
        }
    }

    @CommandLine.Command(
        name = "migrate-h2",
        description = "Migrates a database written by H2 1.x to the new H2 database format. " +
            "The original database file is kept as <database>" + H2FormatUtils.LEGACY_BACKUP_SUFFIX + ". " +
            "The controller must be stopped while the migration runs."
    )
    private static class CmdMigrateH2 implements Callable<Object>
    {
        private static final int EXIT_CODE_FAILED = 1;
        private static final int EXIT_CODE_ERROR = 2;
        private static final int EXIT_CODE_DB_IN_USE = 3;
        private static final int EXIT_CODE_MIGRATION_NEEDED = 100;

        @CommandLine.Option(names = {"-c", "--config-directory"},
            description = "Configuration directory for the controller"
        )
        private String configurationDirectory = "/etc/linstor";

        @CommandLine.Option(names = {"-l", "--logs"},
            description = "Path to the log directory"
        )
        private @Nullable String logDirectory = null;

        @CommandLine.Option(names = {"--check-only"},
            description = "Only check whether a migration is needed, exit code 100 means migration needed"
        )
        private boolean checkOnly = false;

        @CommandLine.Option(names = {"-y", "--yes"},
            description = "Do not ask for confirmation before migrating"
        )
        private boolean yes = false;

        @CommandLine.Option(names = {"--h2-jar"},
            description = "Path to the H2 1.x jar used to read the old database"
        )
        private @Nullable String h2Jar = null;

        @Override
        public Object call() throws Exception
        {
            H2MigrationOrchestrator orchestrator = new H2MigrationOrchestrator(
                configurationDirectory,
                logDirectory,
                System.out,
                System.err
            );
            switch (orchestrator.check())
            {
                case NOT_NEEDED ->
                    System.out.println("The configured database does not need a H2 migration.");
                case DB_IN_USE ->
                {
                    System.err.println(
                        "The database is currently in use. Stop the linstor-controller service before migrating."
                    );
                    System.exit(EXIT_CODE_DB_IN_USE);
                }
                case INTERRUPTED ->
                {
                    System.err.println(
                        "A previous migration did not finish: the database file is missing, but a " +
                            H2FormatUtils.LEGACY_BACKUP_SUFFIX + " file exists. " +
                            "Restore the backup file (remove the " + H2FormatUtils.LEGACY_BACKUP_SUFFIX +
                            " suffix) and run the migration again."
                    );
                    System.exit(EXIT_CODE_ERROR);
                }
                case NEEDED ->
                {
                    if (checkOnly)
                    {
                        System.out.println("The database needs to be migrated to the new H2 format.");
                        System.exit(EXIT_CODE_MIGRATION_NEEDED);
                    }
                    if (!yes && !confirm())
                    {
                        System.err.println("Migration aborted.");
                        System.exit(EXIT_CODE_ERROR);
                    }
                    if (!orchestrator.migrate(h2Jar != null ? Paths.get(h2Jar) : null))
                    {
                        System.exit(EXIT_CODE_FAILED);
                    }
                }
                default -> throw new IllegalStateException("Unknown migration check result");
            }
            return null;
        }

        private boolean confirm() throws IOException
        {
            System.out.print("Migrate the database to the new H2 format? [y/N]: ");
            System.out.flush();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            @Nullable String line = reader.readLine();
            return line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
        }
    }

    private static void runDbExportImport(
        String cfgPath,
        @Nullable String logDirectory,
        boolean enableMigrationOnInit,
        boolean requireAllMigrationsApplied,
        Consumer<Injector> injectorConsumer
    )
        throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException,
        SystemServiceStartException, InitializationException, DatabaseException
    {
        List<String> cfgArgs = new ArrayList<>(Arrays.asList("-c", cfgPath));
        if (logDirectory != null)
        {
            cfgArgs.add("--logs");
            cfgArgs.add(logDirectory);
        }
        CtrlConfig cfg = new CtrlConfig(cfgArgs.toArray(new String[0]));

        ErrorReporter errorLog = new StdErrorReporter(
            "linstor-db",
            Paths.get(cfg.getLogDirectory()),
            cfg.isLogPrintStackTrace(),
            "",
            cfg.getLogLevel(),
            cfg.getLogLevelLinstor()
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
        final List<Module> injModList = new ArrayList<>(
            Arrays.asList(
                new GuiceConfigModule(),
                new LoggingModule(errorLog),
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
            dbInit.setEnableMigrationOnInit(enableMigrationOnInit);
            dbInit.initialize();

            // the export only knows the current schema and would label old data as current
            if (requireAllMigrationsApplied && dbInit.needsMigration())
            {
                dbInit.shutdown(true);
                System.err.println(
                    "The database has pending migrations and cannot be exported by this LINSTOR version. " +
                        "Either export it with the LINSTOR version that last ran on this database, or pass " +
                        "--migrate-before-export."
                );
                System.exit(EXIT_CODE_MIGRATION_PENDING);
            }

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
