package com.linbit.linstor.dbcp.migration;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.ConfigurationAware;
import org.flywaydb.core.api.configuration.FlywayConfiguration;
import org.flywaydb.core.api.migration.MigrationInfoProvider;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

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
public abstract class LinstorMigration implements JdbcMigration, MigrationInfoProvider, ConfigurationAware
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
    public boolean isUndo()
    {
        return false;
    }

    @Override
    public void setFlywayConfiguration(FlywayConfiguration flywayConfiguration)
    {
        dbType = flywayConfiguration.getPlaceholders().get(PLACEHOLDER_KEY_DB_TYPE);
    }

    protected String getDbType()
    {
        return dbType;
    }
}
