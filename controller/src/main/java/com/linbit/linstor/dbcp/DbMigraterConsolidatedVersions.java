package com.linbit.linstor.dbcp;

import com.linbit.linstor.InitializationException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.LinstorMigrationVersion;

import java.util.HashMap;

public class DbMigraterConsolidatedVersions
{
    private static final HashMap<String, String> MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION = new HashMap<>();

    static
    {
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.03.27.14.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.04.16.12.30", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.05.23.16.53", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.05.30.15.55", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.06.08.14.45", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.06.13.10.26", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.08.20.17.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.09.03.14.30", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.10.08.13.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.10.19.10.47", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.11.22.10.53", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2018.12.13.14.32", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.01.17.10.48", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.02.20.09.26", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.03.06.09.10", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.03.06.14.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.03.14.09.10", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.03.15.07.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.04.04.09.53", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.04.10.08.47", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.05.27.13.44", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.06.25.14.27", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.07.09.10.42", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.07.23.13.37", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.09.09.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.10.01.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.10.17.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.10.31.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.11.04.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.11.12.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.11.21.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.12.03.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.12.04.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2019.12.11.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.01.20.15.42", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.02.07.09.30", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.02.12.09.30", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.03.09.13.37", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.03.24.01.01", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.04.01.13.37", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.05.14.13.37", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.06.03.10.37", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.07.09.08.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.07.23.10.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.08.20.10.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.09.30.16.15", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.10.13.12.40", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.10.22.13.42", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2020.12.03.10.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.02.24.12.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.03.30.12.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.04.12.12.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.04.14.12.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.04.29.12.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.04.29.13.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.05.03.13.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.05.07.08.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.05.12.09.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.05.26.09.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2021.06.25.09.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2022.01.16.12.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2022.01.24.14.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2022.01.25.09.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2022.03.23.09.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2022.07.27.09.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2022.08.05.08.00", "v1.33.2");
        MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.put("2022.10.03.09.00", "v1.33.2");
        // migration 2022.11.14.09.00 was the last we consolidated. If the user has that already applied
        // the current version of LINSTOR can continue from here (currently with migration 2023.*)
    }

    /**
     * <p>Checks if the given {@code lastAppliedMigrationRef} can be used to continue the migration chain.</p>
     * <p>This method throws a {@link InitializationException} if the given {@code lastAppliedMigrationRef} was
     * registered as being too old, meaning that the migration itself was already consolidated and we know that some
     * migrations are missing between the current applied migration and our first non-intial migration. That means that
     * we cannot continue migration at this point.</p>
     *
     * @throws InitializationException if the current database version is too old to be migrated.
     */
    public static void checkIfMigrationIsPossible(LinstorMigrationVersion lastAppliedMigrationRef)
        throws InitializationException
    {
        @Nullable String lastLinstorVersionSupportingMigration;
        lastLinstorVersionSupportingMigration = MIGRATION_SUPPORTED_BY_LAST_LINSTOR_VERSION.get(
            lastAppliedMigrationRef.toString()
        );

        if (lastLinstorVersionSupportingMigration != null)
        {
            throw new InitializationException(
                "Local DB version (" + lastAppliedMigrationRef +
                    ") is too old. The last version of LINSTOR that supports migration of local DB is " +
                    lastLinstorVersionSupportingMigration
            );
        }
    }

    private DbMigraterConsolidatedVersions()
    {
    }

}
