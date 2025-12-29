package com.linbit.linstor.dbcp;

import com.linbit.ImplementationError;
import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.ClassPathLoader;
import com.linbit.linstor.dbcp.migration.LinstorMigration;
import com.linbit.linstor.dbcp.migration.LinstorMigrationVersion;
import com.linbit.linstor.dbcp.migration.Migration;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SQLUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.DatabaseInfo.DB2_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.DERBY_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.H2_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.INFORMIX_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.MARIADB_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.MYSQL_MIN_VERSION;
import static com.linbit.linstor.DatabaseInfo.POSTGRES_MIN_VERSION;
import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;


public class DbMigrater
{
    private final ErrorReporter logger;

    public DbMigrater(
        ErrorReporter errLogRef
    )
    {
        logger = errLogRef;
    }

    private TreeMap<LinstorMigrationVersion, LinstorMigration> buildMigrations()
    {
        ClassPathLoader classPathLoader = new ClassPathLoader(logger);
        List<Class<? extends LinstorMigration>> sqlMigrationClasses = classPathLoader.loadClasses(
            LinstorMigration.class.getPackage().getName(),
            Collections.singletonList(""),
            LinstorMigration.class,
            Migration.class
        );

        TreeMap<LinstorMigrationVersion, LinstorMigration> migrations = new TreeMap<>();
        try
        {
            for (Class<? extends LinstorMigration> sqlMigrationClass : sqlMigrationClasses)
            {
                LinstorMigration migration = sqlMigrationClass.getDeclaredConstructor().newInstance();
                LinstorMigrationVersion version = migration.getVersion();
                if (migrations.put(version, migration) != null)
                {
                    throw new ImplementationError(
                        "Duplicated migration version: " + version + ". " +
                            migrations.get(version).getDescription() + " " +
                            migration.getDescription()
                    );
                }
            }
        }
        catch (InstantiationException | IllegalAccessException exc)
        {
            throw new ImplementationError("Failed to load migrations for SQL", exc);
        }
        catch (InvocationTargetException | NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }

        return migrations;
    }

    private DatabaseInfo.DbProduct getDbProduct(Connection conn) throws DatabaseException
    {
        try
        {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            String dbProductName = databaseMetaData.getDatabaseProductName();
            String dbProductVersion = databaseMetaData.getDatabaseProductVersion();

            return DatabaseInfo.getDbProduct(dbProductName, dbProductVersion);
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
    }

    private void runMigrations(
        Connection conn,
        TreeMap<LinstorMigrationVersion, LinstorMigration> migrations,
        LinstorMigrationVersion targetVersion)
        throws Exception
    {
        final DatabaseInfo.DbProduct dbProduct = getDbProduct(conn);
        DatabaseDriverInfo databaseInfo = DatabaseDriverInfo.createDriverInfo(dbProduct.dbType());
        int startRank = getHighestAppliedRank(conn, databaseInfo);
        try (var stmtVersionHistory = conn.prepareStatement(databaseInfo.versionTableInsertStatement()))
        {

            long startTime = System.currentTimeMillis();
            int rank = startRank;
            int count = 0;
            for (Map.Entry<LinstorMigrationVersion, LinstorMigration> entry : migrations.entrySet())
            {
                LinstorMigrationVersion curVersion = entry.getKey();
                LinstorMigration migration = entry.getValue();
                logger.logDebug("Migrating DB: " + migration.getVersion() + ": " + migration.getDescription());
                long start = System.currentTimeMillis();
                migration.migrate(conn, dbProduct);
                long runtime = System.currentTimeMillis() - start;

                stmtVersionHistory.setInt(1, ++rank);
                stmtVersionHistory.setString(2, migration.getVersion().toString());
                stmtVersionHistory.setString(3, migration.getDescription());
                stmtVersionHistory.setString(4, migration.getClass().getName());
                stmtVersionHistory.setInt(5, (int) runtime);
                stmtVersionHistory.execute();

                conn.commit();
                count++;
                if (curVersion == targetVersion)
                {
                    break;
                }
            }

            logger.logInfo("Applied %d migrations in %dms", count, System.currentTimeMillis() - startTime);
        }
    }

    private TreeMap<LinstorMigrationVersion, LinstorMigration> getNeededMigrations(
        TreeMap<LinstorMigrationVersion, LinstorMigration> migrations,
        TreeSet<LinstorMigrationVersion> appliedVersions)
    {
        var migrationsCopy = new TreeMap<>(migrations);
        for (var version : appliedVersions)
        {
            migrationsCopy.remove(version);
        }
        return migrationsCopy;
    }

    private TreeSet<LinstorMigrationVersion> getAppliedVersions(Connection conn, DatabaseDriverInfo dbInfo)
    {
        var versionSet = new TreeSet<LinstorMigrationVersion>();
        try(var stmt = conn.createStatement())
        {
            var result = stmt.executeQuery(dbInfo.queryVersionsStatement());
            while (result.next())
            {
                versionSet.add(LinstorMigrationVersion.fromVersion(result.getString("version")));
            }

        }
        catch (SQLException sqlExc)
        {
            // if the FLYWAY_SCHEMA_HISTORY isn't created yet, it will throw an exception
            logger.logError("Error getting DB version: " + sqlExc);
        }
        return versionSet;
    }

    private int getHighestAppliedRank(Connection conn, DatabaseDriverInfo dbInfo)
    {
        int rank = -2;
        try(var stmt = conn.createStatement())
        {
            var result = stmt.executeQuery(dbInfo.getDbVersionHighestRankStmt());
            if (result.next())
            {
                rank = result.getInt("installed_rank");
            }
        }
        catch (SQLException sqlExc)
        {
            // if the FLYWAY_SCHEMA_HISTORY isn't created yet, it will throw an exception
            logger.logError("Error checking current DB version: " + sqlExc);
        }
        return rank;
    }

    public void migrateToVersion(Connection conn, DatabaseDriverInfo dbInfo, @Nullable String targetVersionRef)
    {
        TreeMap<LinstorMigrationVersion, LinstorMigration> migrations = buildMigrations();
        TreeMap<LinstorMigrationVersion, LinstorMigration> neededMigrations =
            getNeededMigrations(migrations, getAppliedVersions(conn, dbInfo));

        if (!neededMigrations.isEmpty())
        {
            LinstorMigrationVersion targetVersion;
            try
            {
                if (targetVersionRef == null)
                {
                    targetVersion = neededMigrations.lastKey();
                }
                else
                {
                    targetVersion = LinstorMigrationVersion.fromVersion(targetVersionRef);

                    @Nullable LinstorMigration targetMigration = neededMigrations.get(targetVersion);
                    if (targetMigration == null)
                    {
                        throw new InitializationException(
                            "Target migration version '" + targetVersionRef + "' does not exist"
                        );
                    }
                }

                runMigrations(conn, neededMigrations, targetVersion);
            }
            catch (Exception exc)
            {
                throw new LinStorDBRuntimeException("Exception occurred during migration", exc);
            }
        }
    }

    private void checkMinVersion(Connection conn) throws InitializationException
    {
        try
        {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            String dbProductName = databaseMetaData.getDatabaseProductName();
            String dbProductVersion = databaseMetaData.getDatabaseProductVersion();

            // check if minimum version requirements of certain databases are satisfied
            int[] dbProductMinVersion = {};
            final DatabaseInfo.DbProduct dbProd = DatabaseInfo.getDbProduct(dbProductName, dbProductVersion);
            switch (dbProd)
            {
                case H2:
                    dbProductMinVersion = H2_MIN_VERSION;
                    break;
                case DERBY:
                    dbProductMinVersion = DERBY_MIN_VERSION;
                    break;
                case DB2:
                    dbProductMinVersion = DB2_MIN_VERSION;
                    break;
                case POSTGRESQL:
                    dbProductMinVersion = POSTGRES_MIN_VERSION;
                    break;
                case MYSQL:
                    dbProductMinVersion = MYSQL_MIN_VERSION;
                    break;
                case MARIADB:
                    dbProductMinVersion = MARIADB_MIN_VERSION;
                    break;
                case INFORMIX:
                    dbProductMinVersion = INFORMIX_MIN_VERSION;
                    break;
                case ASE: // fall-through
                case DB2_I: // fall-through
                case DB2_Z: // fall-through
                case MSFT_SQLSERVER: // fall-through
                case ORACLE_RDBMS: // fall-through
                case UNKNOWN: // fall-through
                default:
                    // currently no other databases with minimum version requirement
                    break;
            }

            if (dbProductMinVersion.length > 0)
            {
                final String[] versionNumberSplit = dbProductVersion.split("\\s");
                if (versionNumberSplit.length > 0)
                {
                    String[] currVersionSplit = versionNumberSplit[0].split("\\.");
                    int currVersionMajor = Integer.parseInt(currVersionSplit[0]);
                    int currVersionMinor = Integer.parseInt(currVersionSplit[1]);
                    int minVersionMajor = dbProductMinVersion[0];
                    int minVersionMinor = dbProductMinVersion[1];

                    if (
                        currVersionMajor < minVersionMajor ||
                            currVersionMajor == minVersionMajor && currVersionMinor < minVersionMinor
                    )
                    {
                        throw new InitializationException(
                            StringUtils.join(
                                "",
                                "Currently installed version (",
                                currVersionMajor + "." + currVersionMinor,
                                ") of database '", dbProductName,
                                "' is older than the required minimum version (",
                                minVersionMajor + "." + minVersionMinor, ")!"
                            )
                        );
                    }
                    else
                    {
                        // Everything is fine so we can proceed with the migration process
                        logger.logInfo("SQL database is %s %s", dbProd.displayName(), dbProductVersion);
                    }
                }
                else
                {
                    throw new InitializationException(
                        "Failed to verify minimal database version! You can try to run linstor-controller without " +
                            "database version check"
                    );
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new InitializationException("Failed to verify minimal database version!", sqlExc);
        }
    }

    public void migrate(Connection conn, DatabaseDriverInfo dbInfo, boolean withStartupVer)
        throws InitializationException
    {
        setSchema(conn, dbInfo);

        if (withStartupVer)
        {
            checkMinVersion(conn);
        }

        migrateToVersion(conn, dbInfo, null);
    }

    public boolean needsMigration(Connection conn, String dbType)
    {
        DatabaseDriverInfo dbInfo = DatabaseDriverInfo.createDriverInfo(dbType);
        TreeMap<LinstorMigrationVersion, LinstorMigration> migrations = buildMigrations();
        var neededMigrations = getNeededMigrations(migrations, getAppliedVersions(conn, dbInfo));
        return !neededMigrations.isEmpty();
    }

    public void setSchema(Connection conn, DatabaseDriverInfo dbInfo)
    {
        try
        {
            conn.setAutoCommit(false);
            SQLUtils.executeStatement(conn, dbInfo.createSchemaStatement());
            conn.setSchema(DATABASE_SCHEMA_NAME);
            try (var stmt = conn.createStatement())
            {
                stmt.execute(dbInfo.createVersionTableStatement());
            }
        }
        catch (SQLException exc)
        {
            throw new LinStorDBRuntimeException("Failed to set transaction isolation or create schema", exc);
        }
    }
}
