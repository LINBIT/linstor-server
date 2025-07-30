package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.annotation.Nullable;

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
public abstract class LinstorMigration extends AbsMigration
{
    public static final String PLACEHOLDER_KEY_DB_TYPE = "dbType";

    private @Nullable String dbType;

    public LinstorMigrationVersion getVersion()
    {
        return LinstorMigrationVersion.fromVersion(getClass().getAnnotation(Migration.class).version());
    }

    public String getDescription()
    {
        return getClass().getAnnotation(Migration.class).description();
    }

    protected @Nullable String getDbType()
    {
        // FIXME: May want to use DatabaseInfo.DbProduct instead in the future
        return dbType;
    }

    public abstract void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception;
}
