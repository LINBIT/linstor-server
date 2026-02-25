# LINSTOR Development How-To Guides

## How to Create a New Database Table

LINSTOR supports multiple database backends: SQL databases (H2, MariaDB, PostgreSQL) and Kubernetes CRDs. When adding a new
table, you need to create migrations for both systems, SQL and K8s.

### Step 1: Create the SQL Migration

Create a Java migration class in `controller/src/main/java/com/linbit/linstor/dbcp/migration/`.

Naming convention: `Migration_YYYY_MM_DD_HH_mm_Description.java`

The `@Migration` annotation requires:
- `version`: Dotted timestamp format `yyyy.MM.dd.HH.mm` (allows branch-based development without collisions)
- `description`: Informative description (should not change)

**For extending existing tables**, use `MigrationUtils` directly.

> ❗ **IMPORTANT:** Always use `MigrationUtils.replaceTypesByDialect()` to ensure SQL type compatibility across different database backends (H2, MariaDB, PostgreSQL).

```java
@Migration(
    version = "2026.04.21.14.00",
    description = "Add column to existing table"
)
public class Migration_2026_04_21_14_00_AddColumn extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.columnExists(connection, "MY_TABLE", "NEW_COLUMN"))
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.replaceTypesByDialect(dbProduct, "ALTER TABLE MY_TABLE ADD NEW_COLUMN VARCHAR(256)")
            );
            // populate newly added column here if needed
        }
    }
}
```

**For creating new tables**, create a dedicated `.sql` file and load it from the migration:

SQL file (`controller/src/main/resources/com/linbit/linstor/dbcp/migration/2026_04_21_add-my-table.sql`):

```sql
CREATE TABLE MY_TABLE(
    UUID CHARACTER(36) NOT NULL,
    NAME VARCHAR(256) NOT NULL PRIMARY KEY,
    DSP_NAME VARCHAR(256) NOT NULL,
    FLAGS BIGINT NOT NULL,
    ...
);
```

Java migration class:

```java
@Migration(
    version = "2026.04.21.14.00",
    description = "Add my table"
)
public class Migration_2026_04_21_AddMyTable extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        // Although migrations should never be applied twice, this is an additional safeguard
        if (!MigrationUtils.tableExists(connection, "MY_TABLE"))
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.replaceTypesByDialect(dbProduct, MigrationUtils.loadResource("2026_04_21_add-my-table.sql"))
            );
        }
    }
}
```

### Step 2: Regenerate Database Constants

Run the following command to generate the new `GenCrdV*` class and update `GeneratedDatabaseTables`:

```sh
make generate-db-constants
```

This updates:
- `server/generated-dbdrivers/com/linbit/linstor/dbdrivers/GeneratedDatabaseTables.java` - Type-safe constants for tables and columns
- `server/generated-dbdrivers/com/linbit/linstor/dbdrivers/k8s/crd/GenCrdV*` (new Kubernetes CRD version)

### Step 3: Create the Kubernetes CRD Migration

Create a migration class in `controller/src/main/java/com/linbit/linstor/dbcp/migration/k8s/crd/` with a naming
convention of `Migration_XX_vY_Z_W_Description.java` where:
- `XX` is the sequential migration number.
- `vY_Z_W` matches the newly generated `GenCrdV*` class from step 2.

**For adding new tables** (no data transformation needed) see `Migration_33_v1_33_1_AddAuthTokensTable.java` as a complete example.

**For data transformation** (populating new columns or modifying existing data), see `Migration_32_v1_33_1_ChangeSpaceTrackingFromDateToTimestamp.java` for a complete example, or refer to the template in `Migration_Template.java`.

> 📝 **NOTE:** Migration classes (both SQL and K8s CRD) are automatically discovered and applied by LINSTOR at startup. No manual registration is required.

### Step 4: Create the Model Class

Create your entity class in `server/src/main/java/com/linbit/linstor/core/objects/`.

Model classes typically:
- Implement domain logic and properties.
- May extend `AbsCoreObj<T>` or implement interfaces like `ProtectedObject`.
- Include nested `InitMaps` interfaces for loading related objects.
- Use `StateFlags` for flag-based properties.

### Step 5: Create the Database Driver

Create a `DbDriver` class in `controller/src/main/java/com/linbit/linstor/core/objects/` that:

- Extends `AbsDatabaseDriver` or `AbsProtectedDatabaseDriver`.
- Uses `@Singleton` annotation.
- Sets up column mappers in the constructor using `setColumnSetter(column, lambda)`.
- Implements load/create/delete methods.

Example structure:

```java
@Singleton
public final class MyEntityDbDriver
    extends AbsProtectedDatabaseDriver<MyEntity, MyEntity.InitMaps, Void>
    implements MyEntityCtrlDatabaseDriver
{
    public MyEntityDbDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        DbEngine dbEngine,
        ObjectProtectionFactory objProtFactoryRef,
        // ... other dependencies
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.MY_TABLE, dbEngine, objProtFactoryRef);

        setColumnSetter(UUID, entity -> entity.getUuid().toString());
        setColumnSetter(NAME, entity -> entity.getName().value);
        // ... other columns
    }
}
```

> 📝 **NOTE:** Unlike migration classes, database drivers and model instances are **not** automatically discovered.
They need to be manually linked and registered in the appropriate module/factory classes.

### Summary of Files to Create/Modify

| Step | Component | Location |
|------|-----------|----------|
| 1 | SQL migration class | `controller/src/main/java/com/linbit/linstor/dbcp/migration/Migration_*.java` |
| 1 | SQL file (optional) | `controller/src/main/resources/com/linbit/linstor/dbcp/migration/*.sql` |
| 2 | Generated constants | `server/generated-dbdrivers/...` (auto-generated via `make generate-db-constants`) |
| 3 | K8s CRD migration | `controller/src/main/java/com/linbit/linstor/dbcp/migration/k8s/crd/Migration_*.java` |
| 4 | Model class | `server/src/main/java/com/linbit/linstor/core/objects/MyEntity.java` |
| 5 | DbDriver class | `controller/src/main/java/com/linbit/linstor/core/objects/MyEntityDbDriver.java` |

---

> ⚠️  DISCLAIMER: This page was generated using AI, reviewed and edited by developers.
