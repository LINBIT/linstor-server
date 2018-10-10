package com.linbit.linstor.dbcp.migration;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.Context;
import org.flywaydb.core.api.migration.JavaMigration;

import java.sql.Connection;

/**
 * Base migration class to enable the individual migration classes to be as simple as possible.
 * <p>
 * Migration info is read from the {@link Migration} annotation.
 * <p>
 * Migrations should be idempotent to make it easier to handle exceptional situations where the
 * database structure does not correspond to the schema history table.
 * <p>
 * Under normal circumstances migrations should not be changed once they have been released.
 */
public abstract class LinstorMigration implements JavaMigration
{
    public static final String PLACEHOLDER_KEY_DB_TYPE = "dbType";

    private String dbType;

    @Override
    public MigrationVersion getVersion()
    {
        return MigrationVersion.fromVersion(getClass().getAnnotation(Migration.class).version());
    }

    @Override
    public String getDescription()
    {
        return getClass().getAnnotation(Migration.class).description();
    }

    @Override
    public Integer getChecksum()
    {
        return null;
    }

    @Override
    public boolean isUndo()
    {
        return false;
    }

    @Override
    public boolean canExecuteInTransaction()
    {
        return true;
    }

    @Override
    public void migrate(Context context)
        throws Exception
    {
        dbType = context.getConfiguration().getPlaceholders().get(PLACEHOLDER_KEY_DB_TYPE);
        migrate(context.getConnection());
    }

    protected String getDbType()
    {
        // FIXME: May want to use DatabaseInfo.DbProduct instead in the future
        return dbType;
    }

    protected abstract void migrate(Connection connection)
        throws Exception;
}
