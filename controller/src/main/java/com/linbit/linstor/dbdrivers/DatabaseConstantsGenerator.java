package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.utils.PairNonNull;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public final class DatabaseConstantsGenerator
{
    public static final String DFLT_DBDRIVERS_PACKAGE = "com.linbit.linstor.dbdrivers";
    public static final String DFLT_GEN_DB_TABLES_CLASS_NAME = "GeneratedDatabaseTables";

    public static final String DFLT_K8S_CRD_PACKAGE = "com.linbit.linstor.dbdrivers.k8s.crd";
    public static final String DFLT_K8S_CRD_CLASS_NAME_FORMAT = "GenCrd%s";
    public static final String DFLT_K8S_CRD_MIGRATION_CLASS_NAME_FORMAT = "Migration_%s";
    public static final String DFLT_K8S_CRD_CLASS_NAME_CURRENT = "GenCrdCurrent";
    public static final String DFLT_K8S_CRD_CLASS_NAME = "GenCrd";
    private static final String CRD_LINSTOR_SPEC_INTERFACE_NAME = "LinstorSpec";
    private static final String CRD_PK_DELIMITER = ":";

    public static final String DB_SCHEMA = "LINSTOR";
    public static final String TYPE_TABLE = "TABLE";


    private static final String INTERFACE_NAME_SQL = "DatabaseTable";
    private static final String COLUMN_HOLDER_NAME = "ColumnImpl";
    private static final String COLUMN_INTERFACE_NAME = "Column";
    private static final String INDENT = "    ";
    public static final Pattern VERSION_PATTERN = Pattern.compile(
        "[Vv]([0-9]+)(?:[._]([0-9]+))?(?:[._]([0-9]+))?(.*?)"
    );

    private static final Set<String> IGNORED_TABLES = Collections.singleton(
        "FLYWAY_SCHEMA_HISTORY"
    );

    // If CRG_GROUP ever changes, make sure to check ALL occurrences.
    // At least truncating old imported CRDs might still depend on this
    public static final String CRD_GROUP = "internal.linstor.linbit.com";

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private Random random = new Random();
    private StringBuilder mainBuilder = new StringBuilder();
    private int indentLevel = 0;
    private TreeMap<String, Table> tbls = new TreeMap<>();
    private List<String> tblsOrder;

    public DatabaseConstantsGenerator(Connection conRef) throws SQLException
    {
        PairNonNull<TreeMap<String, Table>, List<String>> pair = extractTables(conRef, IGNORED_TABLES);
        tbls = pair.objA;
        tblsOrder = pair.objB;
    }

    public static PairNonNull<TreeMap<String, Table>, List<String>> extractTables(
        Connection con,
        Set<String> ignoredTables
    )
        throws SQLException
    {
        TreeMap<String, Table> tables = new TreeMap<>();
        List<String> crossRefOrder = new ArrayList<>();
        try
            (
                ResultSet metaTables = con.getMetaData().getTables(
                    null,
                    DB_SCHEMA,
                    null,
                    new String[]{TYPE_TABLE}
                );
                ResultSet crossRefs = con.prepareStatement(
                    "SELECT PKTABLE_NAME, FKTABLE_NAME " +
                        "FROM INFORMATION_SCHEMA.CROSS_REFERENCES " +
                        "WHERE PKTABLE_SCHEMA = 'LINSTOR' "
                ).executeQuery();
            )
        {
            HashMap<String, Set<String>> references = new HashMap<>();
            TreeSet<String> startingTables = new TreeSet<>();
            references.put(null, startingTables);

            while (metaTables.next())
            {
                String tblName = metaTables.getString("TABLE_NAME");
                if (!ignoredTables.contains(tblName))
                {
                    startingTables.add(tblName);
                    Table tbl = new Table(tblName);

                    Set<String> primaryKeys = new TreeSet<>();
                    try (ResultSet primaryKeyResultSet = con.getMetaData().getPrimaryKeys(null, DB_SCHEMA, tblName))
                    {
                        while (primaryKeyResultSet.next())
                        {
                            primaryKeys.add(primaryKeyResultSet.getString("COLUMN_NAME"));
                        }
                    }

                    try (
                        ResultSet metaColumns = con.getMetaData().getColumns(
                            null,
                            DB_SCHEMA,
                            tblName,
                            null
                        )
                    )
                    {
                        while (metaColumns.next())
                        {
                            String clmName = metaColumns.getString("COLUMN_NAME");
                            tbl.columns.add(
                                new Column(
                                    tbl,
                                    clmName,
                                    metaColumns.getString("TYPE_NAME"),
                                    primaryKeys.contains(clmName),
                                    metaColumns.getString("IS_NULLABLE").equalsIgnoreCase("yes")
                                )
                            );
                        }
                    }
                    tables.put(tbl.name, tbl);
                }
            }

            while (crossRefs.next())
            {
                String dstTableName = crossRefs.getString("PKTABLE_NAME");
                String srcTableName = crossRefs.getString("FKTABLE_NAME");

                references.computeIfAbsent(srcTableName, ignored -> new TreeSet<>()).add(dstTableName);
                startingTables.remove(dstTableName);
            }

            buildCrossRefOrderRec(
                crossRefOrder,
                references,
                startingTables,
                new HashSet<>()
            );
        }
        return new PairNonNull<>(tables, crossRefOrder);
    }

    private static void buildCrossRefOrderRec(
        List<String> outputListRef,
        HashMap<String, Set<String>> referencesRef,
        Set<String> nextToAppendRef,
        HashSet<String> visitedSetRef
    )
    {
        if (nextToAppendRef != null)
        {
            for (String next : nextToAppendRef)
            {
                if (visitedSetRef.add(next))
                {
                    buildCrossRefOrderRec(outputListRef, referencesRef, referencesRef.get(next), visitedSetRef);
                    outputListRef.add(next);
                }
            }
        }
    }

    public String renderSqlConsts(String pkgName, String clazzName)
    {
        mainBuilder = new StringBuilder();

        renderPackageAndImports(
            pkgName,
            "com.linbit.ImplementationError",
            "com.linbit.linstor.annotation.Nullable",
            "com.linbit.linstor.dbdrivers.DatabaseTable.Column",
            "", // empty line
            "java.sql.Types"
        );

        appendEmptyLine();
        appendLine("@SuppressWarnings(\"checkstyle:linelength\")");
        appendLine("public class %s", clazzName);
        try (IndentLevel clazzIndent = new IndentLevel())
        {
            appendLine("private %s()", clazzName);
            try (IndentLevel constructor = new IndentLevel())
            {
            }

            appendEmptyLine();
            appendLine("// Schema name");
            appendLine("public static final String DATABASE_SCHEMA_NAME = \"%s\";", DB_SCHEMA);
            appendEmptyLine();

            // tables and columns
            for (Table tbl : tbls.values())
            {
                renderSqlConstsTable(tbl);
                appendEmptyLine();
            }

            // table constants
            appendLine("public static final DatabaseTable[] ALL_TABLES; // initialized in static block");
            for (Table tbl : tbls.values())
            {
                String tblNameCamelCase = toUpperCamelCase(tbl.name);
                appendLine(
                    "public static final %s %s = new %s();",
                    tblNameCamelCase,
                    tbl.name,
                    tblNameCamelCase
                );
            }

            /*
             * static initializer setting .table variable of all Column instance to the
             * corresponding table constant
             */
            appendEmptyLine();
            appendLine("static");
            try (IndentLevel staticBlock = new IndentLevel())
            {
                appendLine("ALL_TABLES = new DatabaseTable[] {");
                try (IndentLevel allTablesInitIndent = new IndentLevel("", "", false, false))
                {
                    for (String tblStr : tblsOrder)
                    {
                        Table tbl = tbls.get(tblStr);
                        appendLine("%s,", tbl.name);
                    }
                    cutLastAndAppend(2, "\n");
                }
                appendLine("};", tbls.values().size());
                appendEmptyLine();

                for (Table tbl : tbls.values())
                {
                    String tblNameCamelCase = toUpperCamelCase(tbl.name);
                    for (Column clm : tbl.columns)
                    {
                        appendLine(
                            "%s.%s.table = %s;",
                            tblNameCamelCase,
                            clm.name,
                            tbl.name
                        );
                    }
                }
            }

            /*
             * ColumnImpl class implementing Column interface
             */
            appendEmptyLine();
            generateColumnClass(
                new String[][]{
                    {"String", "name"},
                    {"int", "sqlType"},
                    {"boolean", "isPk"},
                    {"boolean", "isNullable"}
                },
                new String[][]{
                    {INTERFACE_NAME_SQL, "table"}
                }
            );

            appendEmptyLine();
            appendLine("@SuppressWarnings(\"checkstyle:ReturnCount\")");
            appendLine("public static DatabaseTable getByValue(String value)");
            try (IndentLevel getByValueIdent = new IndentLevel())
            {
                appendLine("switch (value.toUpperCase())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return %s;", tbl.name);
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("throw new ImplementationError(\"Unknown database table: \" + value);");
                    }
                }
            }
        }
        return mainBuilder.toString();
    }

    private void renderPackageAndImports(String pkgName, @Nullable String... imports)
    {
        appendLine("package %s;", pkgName);
        appendEmptyLine();
        for (String imp : imports)
        {
            if (imp != null)
            {
                if (imp.isBlank())
                {
                    appendEmptyLine();
                }
                else
                {
                    appendLine("import " + imp + ";");
                }
            }
        }
    }

    private void renderSqlConstsTable(Table tbl)
    {
        String tblNameCamelCase = toUpperCamelCase(tbl.name);

        List<String> primaryKeysFields = new ArrayList<>();
        List<String> otherColumsFields = new ArrayList<>();
        for (Column clm : tbl.columns)
        {
            String line = String.format(
                "public static final %s %s = new %s(\"%s\", Types.%s, %s, %s);",
                COLUMN_HOLDER_NAME,
                clm.name,
                COLUMN_HOLDER_NAME,
                clm.name,
                clm.sqlType,
                clm.pk,
                clm.nullable
            );
            if (clm.pk)
            {
                primaryKeysFields.add(line);
            }
            else
            {
                otherColumsFields.add(line);
            }
        }

        appendLine("public static class %s implements %s", tblNameCamelCase, INTERFACE_NAME_SQL);
        try (IndentLevel tblDfn = new IndentLevel())
        {
            // constructor
            appendLine("private %s()", tblNameCamelCase);
            try (IndentLevel tblClsIndent = new IndentLevel())
            {
                // empty constructor
            }
            appendEmptyLine();

            // primary key(s)
            appendLine("// Primary Key%s", primaryKeysFields.size() > 1 ? "s" : "");
            for (String pkLine : primaryKeysFields)
            {
                appendLine(pkLine);
            }

            // remaining columns
            if (!otherColumsFields.isEmpty())
            {
                appendEmptyLine();
                for (String clmLine : otherColumsFields)
                {
                    appendLine(clmLine);
                }
            }

            // Column[] ALL
            appendEmptyLine();
            appendLine("public static final Column[] ALL = new Column[]");
            try (IndentLevel allColumns = new IndentLevel("{", "};", true, true))
            {
                for (Column clm : tbl.columns)
                {
                    appendLine("%s,", clm.name);
                }
                cutLastAndAppend(2, "\n");
            }

            // values()
            appendEmptyLine();
            appendLine("@Override");
            appendLine("public Column[] values()");
            try (IndentLevel valuesMethod = new IndentLevel())
            {
                appendLine("return ALL;");
            }

            // getName()
            appendEmptyLine();
            appendLine("@Override");
            appendLine("public String getName()");
            try (IndentLevel valuesMethod = new IndentLevel())
            {
                appendLine("return \"%s\";", tbl.name);
            }

            // toString()
            appendEmptyLine();
            appendLine("@Override");
            appendLine("public String toString()");
            try (IndentLevel valuesMethod = new IndentLevel())
            {
                appendLine("return \"Table %s\";", tbl.name);
            }
        }
    }

    /*
     * K8S CRD
     */
    public GeneratedCrdResult renderKubernetesCustomResourceDefinitions(
        String basePkgName,
        String clazzNameFormatRef,
        String currentVersionStrRef,
        Set<String> allVersionsRef,
        String genDbTablesJavaCodeRef
    )
    {
        // reinitialize random with currentVersion based seed
        random = new Random(currentVersionStrRef.hashCode());

        mainBuilder = new StringBuilder();
        TreeSet<GeneratorVersion> olderVersions = new TreeSet<>();
        for (String olderVersion : allVersionsRef)
        {
            GeneratorVersion version = getVersion(olderVersion);
            olderVersions.add(version);
        }
        GeneratorVersion curVer = getVersion(currentVersionStrRef);
        olderVersions.remove(curVer);

        Set<GeneratedCrdJavaClass> javaClasses = new HashSet<>();
        Set<GeneratedResources> resources = new HashSet<>();

        // currently we are only generating one large class instead of many smaller classes (one per table? or three?)
        renderCurrentGenCrd(basePkgName, currentVersionStrRef, DFLT_K8S_CRD_CLASS_NAME_CURRENT, null);
        String genCrdCurrentCode = mainBuilder.toString();
        javaClasses.add(
            new GeneratedCrdJavaClass(basePkgName, DFLT_K8S_CRD_CLASS_NAME_CURRENT, genCrdCurrentCode)
        );

        // re-render but this time add the "GeneratedDatbaseTables".
        // although we have to render the same class twice, the first version can be used as "GenCrdCurrent"
        // while the second can be used as "GenCrdV...". Only the latter one should include a copy of
        // "GeneratedDatabaseTables" class
        mainBuilder.setLength(0);
        String versionedClassName = String.format(DFLT_K8S_CRD_CLASS_NAME_FORMAT, currentVersionStrRef.toUpperCase());
        renderCurrentGenCrd(basePkgName, currentVersionStrRef, versionedClassName, genDbTablesJavaCodeRef);
        String genCrdVersionCode = mainBuilder.toString();

        // also save the just generated code as GenCrd${currentVersion}.java for the migration to refer to
        javaClasses.add(
            new GeneratedCrdJavaClass(basePkgName, versionedClassName, genCrdVersionCode)
        );

        // renderMainGenCrd(basePkgName, clazzNameFormatRef, allVersions, curVer);
        // javaClasses.add(
        // new GeneratedCrdJavaClass(basePkgName, DFLT_K8S_CRD_CLASS_NAME, genCrdCurrentCode)
        // );

        for (Table tbl : tbls.values())
        {
            renderYamlFile(tbl, currentVersionStrRef, olderVersions);
            resources.add(new GeneratedResources(getYamlLocation(tbl, currentVersionStrRef), mainBuilder.toString()));
        }

        // renderMigrationClassTemplate(currentVersionStrRef);
        // GeneratedCrdJavaClass migrationTemplateClass = new GeneratedCrdJavaClass(
        // K8sCrdMigration.class.getPackage().getName(),
        // String.format(DFLT_K8S_CRD_MIGRATION_CLASS_NAME_FORMAT, currentVersionStrRef.toUpperCase()),
        // mainBuilder.toString()
        // );
        return new GeneratedCrdResult(javaClasses, resources);
    }

    private void renderCurrentGenCrd(
        String pkgName,
        String currentVersionRef,
        String clazzName,
        @Nullable String genDbTablesJavaCodeRef
    )
    {
        mainBuilder.setLength(0);
        renderPackageAndImports(
            pkgName,
            // "" will result in empty line, null will be skipped (no empty line)

            // "java.io.Serializable",
            "com.linbit.ImplementationError",
            "com.linbit.linstor.annotation.Nullable",
            "com.linbit.linstor.dbdrivers.DatabaseTable",
            "com.linbit.linstor.dbdrivers.DatabaseTable.Column",
            genDbTablesJavaCodeRef == null ? "com.linbit.linstor.dbdrivers.GeneratedDatabaseTables" : null,
            "com.linbit.linstor.dbdrivers.RawParameters",
            "com.linbit.linstor.security.AccessDeniedException",
            "com.linbit.linstor.transaction.BaseControllerK8sCrdTransactionMgrContext",
            "com.linbit.linstor.transaction.K8sCrdMigrationContext",
            "com.linbit.linstor.transaction.K8sCrdSchemaUpdateContext",
            "com.linbit.linstor.utils.ByteUtils",
            "com.linbit.utils.ExceptionThrowingFunction",
            "com.linbit.utils.TimeUtils",
            "", // empty line
            "java.nio.charset.StandardCharsets",
            genDbTablesJavaCodeRef != null ? "java.sql.Types" : null,
            "java.time.format.DateTimeFormatter",
            // "java.util.ArrayList",
            "java.util.Date",
            "java.util.HashMap",
            "java.util.HashSet",
            "java.util.Map",
            "java.util.TreeMap",
            "java.util.concurrent.atomic.AtomicLong",
            "", // empty line
            "com.fasterxml.jackson.annotation.JsonCreator",
            "com.fasterxml.jackson.annotation.JsonIgnore",
            "com.fasterxml.jackson.annotation.JsonInclude",
            "com.fasterxml.jackson.annotation.JsonInclude.Include",
            "com.fasterxml.jackson.annotation.JsonProperty",
            // "com.fasterxml.jackson.annotation.JsonTypeInfo",
            "com.fasterxml.jackson.annotation.JsonTypeInfo.Id",
            "com.fasterxml.jackson.databind.DatabindContext",
            "com.fasterxml.jackson.databind.JavaType",
            // "com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver",
            "com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase",
            "com.fasterxml.jackson.databind.type.TypeFactory",
            // "com.fasterxml.jackson.databind.annotation.JsonDeserialize",
            // "io.fabric8.kubernetes.api.model.KubernetesResource",
            // "io.fabric8.kubernetes.api.model.Namespaced",
            // "io.fabric8.kubernetes.api.model.HasMetadata",
            "io.fabric8.kubernetes.api.model.ObjectMeta",
            "io.fabric8.kubernetes.api.model.ObjectMetaBuilder",
            "io.fabric8.kubernetes.client.CustomResource",
            // "io.fabric8.kubernetes.client.CustomResourceList",
            "io.fabric8.kubernetes.model.annotation.Group",
            "io.fabric8.kubernetes.model.annotation.Plural",
            "io.fabric8.kubernetes.model.annotation.Singular",
            "io.fabric8.kubernetes.model.annotation.Version"
        );
        appendEmptyLine();

        appendLine("@GenCrd(");
        try (IndentLevel annotIndent = new IndentLevel("", ")", false, true))
        {
            appendLine("dataVersion = \"%s\"", asYamlVersionString(currentVersionRef));
        }
        appendLine("public class %s", clazzName);
        try (IndentLevel clazzIndent = new IndentLevel())
        {
            appendLine("public static final String VERSION = \"%s\";", asYamlVersionString(currentVersionRef));
            appendLine("public static final String GROUP = \"%s\";", CRD_GROUP);
            appendLine(
                "private static final DateTimeFormatter RFC3339 = " +
                    "DateTimeFormatter.ofPattern(\"yyyy-MM-dd'T'HH:mm:ssXXX\");"
            );
            appendLine("private static final Map<String, String> KEY_LUT = new HashMap<>();");
            appendLine("private static final HashSet<String> USED_K8S_KEYS = new HashSet<>();");
            appendLine("private static final AtomicLong NEXT_ID = new AtomicLong();");
            appendLine("private static final HashMap<String, Class<?>> JSON_ID_TO_TYPE_CLASS_LUT = new HashMap<>();");

            appendEmptyLine();
            appendLine("static");
            try (IndentLevel staticIndent = new IndentLevel())
            {
                for (Table tbl : tbls.values())
                {
                    String upperCamelCaseTblName = toUpperCamelCase(tbl.name);
                    appendLine(
                        "JSON_ID_TO_TYPE_CLASS_LUT.put(\"%s\", %s.class);",
                        upperCamelCaseTblName,
                        upperCamelCaseTblName
                    );
                    appendLine(
                        "JSON_ID_TO_TYPE_CLASS_LUT.put(\"%sSpec\", %sSpec.class);",
                        upperCamelCaseTblName,
                        upperCamelCaseTblName
                    );
                }
            }

            appendEmptyLine();
            appendLine("private %s()", clazzName);
            try (IndentLevel constructor = new IndentLevel())
            {
            }

            // databaseTableToCustomResourceClass
            appendEmptyLine();
            appendLine("@SuppressWarnings(\"unchecked\")");
            appendLine(
                "public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> " +
                    "@Nullable Class<CRD> databaseTableToCustomResourceClass("
            );
            try (IndentLevel databaseTableToCrdIndent = new IndentLevel("", "", false, false))
            {
                appendLine("DatabaseTable table");
            }
            appendLine(")");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("switch (table.getName())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        String tblNameCamelCase = toUpperCamelCase(tbl.name);
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return (Class<CRD>) %s.class;", tblNameCamelCase);
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("// we are most likely iterating tables the current version does not know about.");
                        appendLine("return null;");
                    }
                }
            }
            appendEmptyLine();
            appendLine("@SuppressWarnings(\"unchecked\")");
            appendLine(
                "public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> " +
                    "@Nullable CRD specToCrd(SPEC spec)"
            );
            try (IndentLevel specToCrdMethod = new IndentLevel())
            {
                appendLine("switch (spec.getDatabaseTable().getName())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        String tblNameCamelCase = toUpperCamelCase(tbl.name);
                        String specClassName = tblNameCamelCase + "Spec";
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return (CRD) new %s((%s) spec);", tblNameCamelCase, specClassName);
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("// we are most likely iterating tables the current version does not know about.");
                        appendLine("return null;");
                    }
                }
            }

            // databaseTableToSpecClass
            appendEmptyLine();
            appendLine("@SuppressWarnings(\"unchecked\")");
            appendLine(
                "public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> " +
                    "@Nullable Class<SPEC> databaseTableToSpecClass("
            );
            try (IndentLevel databaseTableToCrdIndent = new IndentLevel("", "", false, false))
            {
                appendLine("DatabaseTable table");
            }
            appendLine(")");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("switch (table.getName())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        String tblNameCamelCase = toUpperCamelCase(tbl.name);
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return (Class<SPEC>) %sSpec.class;", tblNameCamelCase);
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("// we are most likely iterating tables the current version does not know about.");
                        appendLine("return null;");
                    }
                }
            }

            // rawParamToSpec
            appendEmptyLine();
            appendLine("@SuppressWarnings(\"unchecked\")");
            appendLine(
                "public static <CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> " +
                    "@Nullable SPEC rawParamToSpec("
            );
            try (IndentLevel rawParamToSpecIndent = new IndentLevel("", "", false, false))
            {
                appendLine("DatabaseTable tableRef,");
                appendLine("RawParameters rawDataMapRef");
            }
            appendLine(")");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("switch (tableRef.getName())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        String tblNameCamelCase = toUpperCamelCase(tbl.name);
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return (SPEC) %sSpec.fromRawParameters(rawDataMapRef);", tblNameCamelCase);
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("// we are most likely iterating tables the current version does not know about.");
                        appendLine("return null;");
                    }
                }
            }

            appendEmptyLine();
            appendLine("@SuppressWarnings(\"unchecked\")");
            appendLine(
                "public static <DATA, CRD extends LinstorCrd<SPEC>, SPEC extends LinstorSpec<CRD, SPEC>> " +
                    "@Nullable CRD dataToCrd("
            );
            try (IndentLevel genericCreateMethodParams = new IndentLevel("", "", false, false))
            {
                appendLine("DatabaseTable table,");
                appendLine("Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,");
                appendLine("DATA data");
            }
            appendLine(")");
            try (IndentLevel genericCreateMethodThrowsDeclarations = new IndentLevel("", "", false, false))
            {
                appendLine("throws AccessDeniedException");
            }
            try (IndentLevel genericCreateMethod = new IndentLevel())
            {
                appendLine("switch (table.getName())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        String tblNameCamelCase = toUpperCamelCase(tbl.name);
                        String specClassName = tblNameCamelCase + "Spec";
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel())
                        {
                            appendLine("return (CRD) new %s(", specClassName);
                            try (IndentLevel specArgIndent = new IndentLevel("", "", false, false))
                            {
                                for (Column clm : tbl.columns)
                                {
                                    appendLine(
                                        "(%s) setters.get(%s.%s.%s).accept(data),",
                                        getJavaType(clm),
                                        DFLT_GEN_DB_TABLES_CLASS_NAME,
                                        toUpperCamelCase(clm.tbl.name),
                                        clm.name
                                    );
                                    // appendLine("(%s) setters.get(%s.%s).accept(data),", type, clmName);
                                }
                                cutLastAndAppend(2, "\n");
                            }
                            appendLine(").getCrd();");
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("// we are most likely iterating tables the current version does not know about.");
                        appendLine("return null;");
                    }
                }
            }

            appendEmptyLine();
            appendLine("public static @Nullable String databaseTableToYamlLocation(DatabaseTable dbTable)");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("switch (dbTable.getName())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return \"%s\";", getYamlLocation(tbl, currentVersionRef));
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("// we are most likely iterating tables the current version does not know about.");
                        appendLine("return null;");
                    }
                }
            }

            appendEmptyLine();
            appendLine("public static @Nullable String databaseTableToYamlName(DatabaseTable dbTable)");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("switch (dbTable.getName())");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Table tbl : tbls.values())
                    {
                        appendLine("case \"%s\":", tbl.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return \"%s\";", toUpperCamelCase(tbl.name).toLowerCase());
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("// we are most likely iterating tables the current version does not know about.");
                        appendLine("return null;");
                    }
                }
            }

            appendEmptyLine();
            appendLine(
                "public static BaseControllerK8sCrdTransactionMgrContext createTxMgrContext()",
                clazzName
            );
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("return new BaseControllerK8sCrdTransactionMgrContext(");
                try (IndentLevel argsIndent = new IndentLevel("", "", false, false))
                {
                    appendLine("%s::databaseTableToCustomResourceClass,", clazzName);
                    appendLine("GeneratedDatabaseTables.ALL_TABLES,");
                    appendLine("%s.VERSION", clazzName);
                }
                appendLine(");");
            }

            appendEmptyLine();
            appendLine("public static K8sCrdSchemaUpdateContext createSchemaUpdateContext()");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("return new K8sCrdSchemaUpdateContext(");
                try (IndentLevel argsIndent = new IndentLevel("", "", false, false))
                {
                    appendLine("%s::databaseTableToYamlLocation,", clazzName);
                    appendLine("%s::databaseTableToYamlName,", clazzName);
                    appendLine("\"%s\"", asYamlVersionString(currentVersionRef));
                }
                appendLine(");");
            }

            appendEmptyLine();
            appendLine("public static K8sCrdMigrationContext createMigrationContext()");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("return new K8sCrdMigrationContext(createTxMgrContext(), createSchemaUpdateContext());");
            }

            for (Table tbl : tbls.values())
            {
                appendEmptyLine();
                renderCrdTableClass(tbl, currentVersionRef, clazzName);
            }

            appendEmptyLine();
            appendLine("public static final String deriveKey(String formattedPrimaryKey)");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("String sha = KEY_LUT.get(formattedPrimaryKey);");
                appendLine("if (sha == null)");
                try (IndentLevel ifIndent = new IndentLevel())
                {
                    appendLine("synchronized (KEY_LUT)");
                    try (IndentLevel synchIndent = new IndentLevel())
                    {
                        appendLine("sha = KEY_LUT.get(formattedPrimaryKey);");
                        appendLine("if (sha == null)");
                        try (IndentLevel secondIfIndent = new IndentLevel())
                        {
                            appendLine(
                                "sha = ByteUtils.bytesToHex(ByteUtils.checksumSha256(" +
                                    "formattedPrimaryKey.getBytes(StandardCharsets.UTF_8))).toLowerCase();"
                            );
                            appendLine("while (!USED_K8S_KEYS.add(sha))");
                            try (IndentLevel whileIndent = new IndentLevel())
                            {
                                appendLine("String modifiedPk = formattedPrimaryKey + NEXT_ID.incrementAndGet();");
                                appendLine(
                                    "sha = ByteUtils.bytesToHex(ByteUtils.checksumSha256(" +
                                        "modifiedPk.getBytes(StandardCharsets.UTF_8))).toLowerCase();"
                                );
                            }
                            appendLine("KEY_LUT.put(formattedPrimaryKey, sha);");
                        }
                    }
                }
                appendLine("return sha;");
            }

            renderResolver("CrdResolver", "LinstorCrd<?>", "crdClass", "");
            // renderResolver("SpecResolver", "LinstorSpec", "specClass", "Spec");
            if (genDbTablesJavaCodeRef != null)
            {
                copyGenDbTables(genDbTablesJavaCodeRef);
            }
        }
    }

    private void renderResolver(String className, String baseClass, String varName, String typeSuffix)
    {
        appendEmptyLine();
        appendLine("public static class JsonTypeResolver extends TypeIdResolverBase");
        try (IndentLevel clsIndent = new IndentLevel())
        {
            appendLine("private @Nullable JavaType baseType;");

            appendEmptyLine();
            appendLine("@Override");
            appendLine("public void init(JavaType baseTypeRef)");
            try (IndentLevel methodIntend = new IndentLevel())
            {
                appendLine("super.init(baseTypeRef);");
                appendLine("baseType = baseTypeRef;");
            }

            appendEmptyLine();
            appendLine("@Override");
            appendLine("public String idFromValue(Object valueRef)");
            try (IndentLevel methodIntend = new IndentLevel())
            {
                appendLine("return idFromValueAndType(valueRef, valueRef.getClass());");
            }

            appendEmptyLine();
            appendLine("@Override");
            appendLine("public String idFromValueAndType(Object ignored, Class<?> suggestedTypeRef)");
            try (IndentLevel methodIntend = new IndentLevel())
            {
                appendLine("return suggestedTypeRef.getSimpleName();");
            }

            appendEmptyLine();
            appendLine("@Override");
            appendLine("public Id getMechanism()");
            try (IndentLevel methodIntend = new IndentLevel())
            {
                appendLine("return Id.MINIMAL_CLASS;");
            }

            appendEmptyLine();
            appendLine("@Override");
            appendLine("public JavaType typeFromId(DatabindContext contextRef, String idRef)");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("Class<?> typeClass = JSON_ID_TO_TYPE_CLASS_LUT.get(idRef);");
                appendLine("return TypeFactory.defaultInstance().constructSpecializedType(baseType, typeClass);");
            }
        }
    }

    private void copyGenDbTables(String genDbTablesJavaCodeRef)
    {
        appendEmptyLine();

        // we need to skip the first lines like "package" and "import" and unnecessary empty lines
        boolean copy = false;
        String[] lines = genDbTablesJavaCodeRef.split("\n");
        for (String line : lines)
        {
            if (!copy && !line.startsWith("package") && !line.startsWith("import") && !line.isBlank())
            {
                copy = true;
            }
            if (copy)
            {
                String tmp = line.replace("public class", "public static class");
                if (tmp.isBlank())
                {
                    appendEmptyLine();
                }
                else
                {
                    appendLine(tmp);
                }
            }
        }
    }

    private String asYamlVersionString(String currentVersionRef)
    {
        return currentVersionRef.replaceAll("_", "-");
    }

    private GeneratorVersion getVersion(String strVersion)
    {
        Matcher mtc = VERSION_PATTERN.matcher(strVersion);
        if (!mtc.find())
        {
            throw new ImplementationError("Failed to parse version '" + strVersion + "'");
        }
        String major = mtc.group(1);
        String minor = mtc.group(2);
        String patch = mtc.group(3);
        String additional = mtc.group(4);
        return new GeneratorVersion(
            new Version(
                Integer.parseInt(major),
                minor == null || minor.isEmpty() ? null : Integer.parseInt(minor),
                patch == null || patch.isEmpty() ? null : Integer.parseInt(patch),
                additional == null || additional.isEmpty() ? null : additional
            ),
            strVersion
        );
    }

    private void renderCrdTableClass(Table tbl, String crdVersion, String clazzNameRef)
    {
        String tblNameCamelCase = toUpperCamelCase(tbl.name);
        String specClassName = tblNameCamelCase + "Spec";

        /*
         * Create method
         */
        appendLine(
            "public static %s create%s(",
            tblNameCamelCase,
            tblNameCamelCase
        );
        try (IndentLevel paramIndent = new IndentLevel("", "", false, false))
        {
            for (Column clm : tbl.columns)
            {
                String type = getJavaType(clm);
                String clmName = camelCase(clm.name.toLowerCase().toCharArray());
                appendLine("%s %s,", type, clmName);
            }
            cutLastAndAppend(2, "\n");
        }
        appendLine(")");
        try (IndentLevel createMethodIndent = new IndentLevel())
        {
            appendLine("return new %s(", tblNameCamelCase);
            try (IndentLevel crArgIndent = new IndentLevel("", "", false, false))
            {
                appendLine("new %s(", specClassName);
                try (IndentLevel specArgIndent = new IndentLevel("", "", false, false))
                {
                    for (Column clm : tbl.columns)
                    {
                        String clmName = camelCase(clm.name.toLowerCase().toCharArray());
                        appendLine("%s,", clmName);
                    }
                    cutLastAndAppend(2, "\n");
                }
                appendLine(")");
            }
            appendLine(");");
        }

        /*
         * Spec class
         */
        appendEmptyLine();
        appendLine("@LinstorData(");
        try (IndentLevel specClazzIndent = new IndentLevel("", ")", false, true))
        {
            appendLine("tableName = \"%s\"", tbl.name);
        }
        appendLine("@JsonInclude(Include.NON_NULL)");
        appendLine(
            "public static class %s implements %s<%s, %s>",
            specClassName,
            CRD_LINSTOR_SPEC_INTERFACE_NAME,
            tblNameCamelCase,
            specClassName
        );
        try (IndentLevel specClazzIndent = new IndentLevel())
        {
            appendLine("@JsonIgnore private static final long serialVersionUID = %dL;", random.nextLong());

            StringBuilder pkFormat = new StringBuilder();
            StringBuilder pkFormatBackup = new StringBuilder(); // in case a table has no PKs defined, take all columns
            boolean pkFound = false;
            for (Column clm : tbl.columns)
            {
                String stringFormatType = getStringFormatType(clm);
                if (stringFormatType != null)
                {
                    pkFormatBackup.append(stringFormatType);
                    pkFormatBackup.append(CRD_PK_DELIMITER);
                    if (clm.pk)
                    {
                        pkFormat.append(stringFormatType);
                        pkFormat.append(CRD_PK_DELIMITER);
                        pkFound = true;
                    }
                }
            }
            if (!pkFound)
            {
                pkFormat = pkFormatBackup;
            }
            pkFormat.setLength(pkFormat.length() - CRD_PK_DELIMITER.length());
            appendLine("@JsonIgnore private static final String PK_FORMAT = \"%s\";", pkFormat);

            appendEmptyLine();
            appendLine("@JsonIgnore private final String formattedPrimaryKey;");
            appendLine("@JsonIgnore private @Nullable %s parentCrd;", tblNameCamelCase);

            appendEmptyLine();
            if (!pkFound)
            {
                appendLine("// No PK found. Combining ALL columns for K8s key");
            }
            LinkedHashMap<String, String> ctorParameters = new LinkedHashMap<>();
            for (Column clm : tbl.columns)
            {
                String type = getJavaType(clm);
                String lowerCaseClmName = clm.name.toLowerCase();
                String camelCaseClmName = camelCase(lowerCaseClmName.toCharArray());
                appendLine(
                    "@JsonProperty(\"%s\") public final %s %s;%s",
                    lowerCaseClmName,
                    type,
                    camelCaseClmName,
                    clm.pk ? " // PK" : ""
                );
                ctorParameters.put(lowerCaseClmName, type);
            }

            appendEmptyLine();
            appendLine("@JsonIgnore");
            appendLine(
                "public static %s fromRawParameters(RawParameters rawParamsRef)",
                specClassName
            );
            try (IndentLevel fromRawParamMethodIndent = new IndentLevel())
            {
                if (ctorParameters.isEmpty())
                {
                    appendLine("return new %s();", specClassName);
                }
                else
                {
                    appendLine("return new %s(", specClassName);
                    try (IndentLevel columnsIndent = new IndentLevel("", "", false, false))
                    {
                        for (Column clm : tbl.columns)
                        {
                            appendLine(
                                "rawParamsRef.getParsed(GeneratedDatabaseTables.%s.%s),",
                                tblNameCamelCase,
                                clm.name
                            );
                        }
                        cutLastAndAppend(2, "\n");
                    }
                    appendLine(");");
                }
            }
            appendEmptyLine();

            // constructor
            if (ctorParameters.isEmpty())
            {
                appendLine("@JsonCreator");
                appendLine("public %s() {}", specClassName);
            }
            else
            {
                appendLine("@JsonCreator");
                appendLine("public %s(", specClassName);
                try (IndentLevel ctorParamIndent = new IndentLevel("", "", false, false))
                {
                    for (Entry<String, String> arg : ctorParameters.entrySet())
                    {
                        String lowerCaseClmName = arg.getKey();
                        String camelCaseClmName = camelCase(lowerCaseClmName.toCharArray());
                        appendLine(
                            "@JsonProperty(\"%s\") %s %sRef,",
                            lowerCaseClmName,
                            arg.getValue(),
                            camelCaseClmName
                        );
                    }
                    cutLastAndAppend(2, "\n");
                }
                appendLine(")");
                try (IndentLevel ctorIndent = new IndentLevel())
                {
                    for (String lowerCaseClmName : ctorParameters.keySet())
                    {
                        String camelCaseClmName = camelCase(lowerCaseClmName.toCharArray());
                        appendLine("%s = %sRef;", camelCaseClmName, camelCaseClmName);
                    }
                    appendEmptyLine();
                    appendLine("formattedPrimaryKey = String.format(");
                    try (IndentLevel argsIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine("%s.PK_FORMAT,", specClassName);
                        for (Column clm : tbl.columns)
                        {
                            if (clm.pk || !pkFound)
                            {
                                String clmName = camelCase(clm.name.toLowerCase().toCharArray());
                                if (clm.sqlType.equals("DATE"))
                                {
                                    appendLine(
                                        "RFC3339.format(TimeUtils.toLocalZonedDateTime(%s.getTime())),",
                                        clmName
                                    );
                                }
                                else
                                {
                                    appendLine("%s,", clmName);
                                }
                            }
                        }
                        cutLastAndAppend(2, "\n");
                    }
                    appendLine(");");
                }
            }

            appendEmptyLine();
            appendLine("@JsonIgnore");
            appendLine("@Override");
            appendLine("public %s getCrd()", tblNameCamelCase);
            try (IndentLevel getCrdMethodIndent = new IndentLevel())
            {
                appendLine("if (parentCrd == null)");
                try (IndentLevel ifNullIndent = new IndentLevel())
                {
                    appendLine("parentCrd = new %s(this);", tblNameCamelCase);
                }

                appendLine("return parentCrd;");
            }

            appendEmptyLine();
            appendLine("@JsonIgnore");
            appendLine("@Override");
            appendLine("public Map<String, Object> asRawParameters()");
            try (IndentLevel getRawParamMethodIndent = new IndentLevel())
            {
                appendLine("Map<String, Object> ret = new TreeMap<>();");
                for (Column clm : tbl.columns)
                {
                    appendLine("ret.put(\"%s\", %s);", clm.name, camelCase(clm.name.toLowerCase().toCharArray()));
                }
                appendLine("return ret;");
            }

            appendEmptyLine();
            appendLine("@JsonIgnore");
            appendLine("@Override");
            appendLine("@Nullable");
            appendLine("public Object getByColumn(String clmNameStr)");
            try (IndentLevel getByColumnMethodIndent = new IndentLevel())
            {
                appendLine("switch (clmNameStr)");
                try (IndentLevel switchIndent = new IndentLevel())
                {
                    for (Column clm : tbl.columns)
                    {
                        appendLine("case \"%s\":", clm.name);
                        try (IndentLevel caseIndent = new IndentLevel("", "", false, false))
                        {
                            appendLine("return %s;", camelCase(clm.name.toLowerCase().toCharArray()));
                        }
                    }
                    appendLine("default:");
                    try (IndentLevel defaultCaseIndent = new IndentLevel("", "", false, false))
                    {
                        appendLine(
                            "throw new ImplementationError(\"Unknown database column. " +
                                "Table: %s, Column: \" + clmNameStr);",
                            tbl.name
                        );
                    }
                }
            }

            appendEmptyLine();
            appendLine("@JsonIgnore");
            appendLine("@Override");
            appendLine("public final String getLinstorKey()");
            try (IndentLevel ctorParamIndent = new IndentLevel())
            {
                appendLine("return formattedPrimaryKey;");
            }


            appendEmptyLine();
            appendLine("@Override");
            appendLine("@JsonIgnore");
            appendLine("public DatabaseTable getDatabaseTable()");
            try (IndentLevel getKeyIndent = new IndentLevel())
            {
                appendLine("return GeneratedDatabaseTables.%s;", tbl.name);
            }
        }

        /*
         * CustomResource class
         */
        appendEmptyLine();
        appendLine("@Version(%s.VERSION)", clazzNameRef);
        appendLine("@Group(%s.GROUP)", clazzNameRef);
        appendLine("@Plural(\"%s\")", tblNameCamelCase.toLowerCase());
        appendLine("@Singular(\"%s\")", tblNameCamelCase.toLowerCase());
        // appendLine("@SuppressWarnings(\"unchecked\")"); // due to interface method '<T> T getSpec();'
        appendLine(
            "public static class %s extends CustomResource<%s, Void> implements LinstorCrd<%s>",
            tblNameCamelCase,
            specClassName,
            specClassName
        );
        try (IndentLevel clazzIndent = new IndentLevel())
        {
            appendLine("private static final long serialVersionUID = %dL;", random.nextLong());
            appendLine("@Nullable String k8sKey = null;");
            appendEmptyLine();

            appendLine("@JsonCreator");
            appendLine("public %s()", tblNameCamelCase);
            try (IndentLevel ctorIndent = new IndentLevel())
            {
            }
            appendEmptyLine();

            appendLine("public %s(%s spec)", tblNameCamelCase, specClassName);
            try (IndentLevel ctorIndent = new IndentLevel())
            {
                appendLine("setMetadata(new ObjectMetaBuilder().withName(deriveKey(spec.getLinstorKey())).build());");
                appendLine("setSpec(spec);");
            }

            appendEmptyLine();
            appendLine("@Override");
            appendLine("public void setSpec(%s specRef)", specClassName);
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("super.setSpec(specRef);");
                appendLine("spec.parentCrd = this;");
            }

            appendEmptyLine();
            appendLine("@Override");
            appendLine("public void setMetadata(ObjectMeta metadataRef)");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("super.setMetadata(metadataRef);");
                appendLine("k8sKey = metadataRef.getName();");
            }

            appendEmptyLine();
            appendLine("@Override");
            appendLine("@JsonIgnore");
            appendLine("public String getLinstorKey()");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("return spec.getLinstorKey();");
            }
            appendEmptyLine();
            appendLine("@Override");
            appendLine("@JsonIgnore");
            appendLine("public @Nullable String getK8sKey()");
            try (IndentLevel methodIndent = new IndentLevel())
            {
                appendLine("return k8sKey;");
            }
        }
    }

    private void renderYamlFile(
        Table tbl,
        String currentVersionRef,
        TreeSet<GeneratorVersion> olderVersionClassNamesRef
    )
    {
        mainBuilder.setLength(0);
        ObjectNode rootNode = YAML_MAPPER.createObjectNode();
        rootNode.put("apiVersion", "apiextensions.k8s.io/v1");
        rootNode.put("kind", "CustomResourceDefinition");

        String nameCamelCase = toUpperCamelCase(tbl.name);
        String nameLowerCase = nameCamelCase.toLowerCase();
        // String namePlural = getNamePlural(nameLowerCase);
        // String nameSingular = getNameSingular(nameLowerCase);
        String namePlural = nameLowerCase;
        String nameSingular = nameLowerCase;

        ObjectNode metadataNode = YAML_MAPPER.createObjectNode();
        {
            metadataNode.put("name", namePlural + "." + CRD_GROUP);
        }
        rootNode.set("metadata", metadataNode);

        ObjectNode specNode = YAML_MAPPER.createObjectNode();
        {
            specNode.put("group", CRD_GROUP);

            ArrayNode versionArrayNode = YAML_MAPPER.createArrayNode();
            {
                // first add the newest (current) version.
                ObjectNode curVerNode = YAML_MAPPER.createObjectNode();
                {
                    curVerNode.put("name", asYamlVersionString(currentVersionRef));
                    curVerNode.put("served", true);
                    curVerNode.put("storage", true); // always true for current version

                    ObjectNode schemaNode = YAML_MAPPER.createObjectNode();
                    {
                        ObjectNode openapiNode = YAML_MAPPER.createObjectNode();
                        {
                            openapiNode.put("type", "object");
                            ObjectNode propertiesNode = YAML_MAPPER.createObjectNode();
                            {
                                ObjectNode propertiesSpecNode = buildPropertiesSpecNode(tbl);
                                propertiesNode.set("spec", propertiesSpecNode);
                            }
                            openapiNode.set("properties", propertiesNode);
                        }
                        schemaNode.set("openAPIV3Schema", openapiNode);
                    }
                    curVerNode.set("schema", schemaNode);
                    versionArrayNode.add(curVerNode);
                }

                // now load versions section from last last
                if (!olderVersionClassNamesRef.isEmpty())
                {
                    JsonNode oldVersionSection = loadOldVersionSection(tbl, olderVersionClassNamesRef.last());
                    if (oldVersionSection != null)
                    {
                        Iterator<JsonNode> versionIt = oldVersionSection.elements();
                        while (versionIt.hasNext())
                        {
                            ObjectNode oldVerNode = versionIt.next().deepCopy();
                            oldVerNode.put("storage", false); // always for older versions
                            versionArrayNode.add(oldVerNode);
                        }
                    }
                }
            }
            specNode.set("versions", versionArrayNode);
            specNode.put("scope", "Cluster");

            ObjectNode namesNode = YAML_MAPPER.createObjectNode();
            {
                namesNode.put("plural", namePlural);
                namesNode.put("singular", nameSingular);
                namesNode.put("kind", nameCamelCase);
                // no short name (for now?)
            }
            specNode.set("names", namesNode);
        }
        rootNode.set("spec", specNode);

        try
        {
            mainBuilder.append(YAML_MAPPER.writeValueAsString(rootNode));
        }
        catch (JsonProcessingException exc)
        {
            throw new ImplementationError("Failed to serialize yaml content", exc);
        }
    }

    private ObjectNode buildPropertiesSpecNode(Table tbl)
    {
        ObjectNode propertiesSpecNode = YAML_MAPPER.createObjectNode();
        {
            propertiesSpecNode.put("type", "object");
            ObjectNode propertiesSpecPropertiesNode = YAML_MAPPER.createObjectNode();
            {
                for (Column clm : tbl.columns)
                {
                    String clmName = clm.name.toLowerCase(); // keep underscores, but all lower-case
                    ObjectNode clmNode = YAML_MAPPER.createObjectNode();
                    {
                        clmNode.put("type", getYamlType(clm));
                        String format = getYamlFormat(clm);
                        if (format != null)
                        {
                            clmNode.put("format", format);
                        }
                    }
                    propertiesSpecPropertiesNode.set(clmName, clmNode);
                }
            }
            propertiesSpecNode.set("properties", propertiesSpecPropertiesNode);
        }
        return propertiesSpecNode;
    }

    private @Nullable JsonNode loadOldVersionSection(Table tbl, GeneratorVersion generatorVersionRef)
    {
        return loadOldVersionSection(getYamlLocation(tbl, generatorVersionRef.originalVersion));
    }

    private @Nullable JsonNode loadOldVersionSection(String yamlLocation)
    {
        JsonNode versionsObj = null;
        try
        {
            URL url = getClass().getResource(yamlLocation);
            if (url != null)
            {
                File file = new File(url.toURI());
                if (file.exists())
                {
                    // System.out.println("loading: " + file);
                    versionsObj = YAML_MAPPER.readTree(file).get("spec").get("versions");
                }
            }
        }
        catch (IOException | URISyntaxException exc)
        {
            throw new ImplementationError("Failed read old yaml file", exc);
        }
        return versionsObj;
    }

    private String getYamlLocation(Table tblRef, String crdVersionRef)
    {
        return getYamlLocation(crdVersionRef, toUpperCamelCase(tblRef.name));
    }

    public static String getYamlLocation(String dir, String fileName)
    {
        return String.format(
            "/com/linbit/linstor/dbcp/k8s/crd/%s/%s.yaml",
            dir,
            fileName
        );
    }

    private String getJavaType(Column clmRef)
    {
        String ret;
        switch (clmRef.sqlType)
        {
            case "CHAR":
                // fall-through
            case "VARCHAR":
                // fall-through
            case "CLOB":
                ret = "String";
                break;
            case "BIGINT":
                // fall-through
            case "TIMESTAMP":
                ret = clmRef.nullable ? "Long" : "long";
                break;
            case "INTEGER":
                ret = clmRef.nullable ? "Integer" : "int";
                break;
            case "SMALLINT":
                ret = clmRef.nullable ? "Short" : "short";
                break;
            case "BOOLEAN":
                ret = clmRef.nullable ? "Boolean" : "boolean";
                break;
            case "BLOB":
                ret = "byte[]";
                break;
            case "DATE":
                ret = "Date";
                break;
            default:
                throw new ImplementationError("Unknown Type: " + clmRef.sqlType);
        }
        return ret;
    }

    private String getYamlType(Column clmRef)
    {
        String ret;
        switch (clmRef.sqlType)
        {
            case "CHAR":
            case "VARCHAR":
            case "CLOB":
            case "BLOB":
                ret = "string";
                break;
            case "DATE":
            case "BIGINT":
            case "TIMESTAMP":
            case "INTEGER":
            case "SMALLINT":
                ret = "integer";
                break;
            case "BOOLEAN":
                ret = "boolean";
                break;
            default:
                throw new ImplementationError("Unknown Type: " + clmRef.sqlType);
        }
        return ret;
    }

    private @Nullable String getYamlFormat(Column clmRef)
    {
        String ret;
        switch (clmRef.sqlType)
        {
            case "BIGINT":
                ret = "int64";
                break;
            case "INTEGER":
                ret = "int32";
                break;
            case "DATE":
                ret = "int64";
                break;
            case "BLOB":
                ret = "byte"; // base64 encoded
                break;
            default:
                ret = null; // no special format
                break;
        }
        return ret;
    }

    private @Nullable String getStringFormatType(Column clmRef)
    {
        String ret;
        switch (clmRef.sqlType)
        {
            case "CHAR":
            case "VARCHAR":
            case "CLOB":
            case "DATE":
                ret = "%s";
                break;
            case "BIGINT":
            case "TIMESTAMP":
            case "INTEGER":
            case "SMALLINT":
                ret = "%d";
                break;
            case "BOOLEAN":
                ret = "%b";
                break;
            case "BLOB":
                ret = null;
                break;
            default:
                throw new ImplementationError("Unknown Type: " + clmRef.sqlType);
        }
        return ret;
    }

    private void cutLastAndAppend(int cutLen, String appendAfter)
    {
        mainBuilder.setLength(mainBuilder.length() - cutLen);
        mainBuilder.append(appendAfter);
    }

    public static String toUpperCamelCase(String nameRef)
    {
        return camelCase(firstToUpper(nameRef.toLowerCase()).toCharArray());
    }

    public static String firstToUpper(String nameRef)
    {
        char[] ret = nameRef.toCharArray();
        ret[0] = Character.toUpperCase(ret[0]);
        return new String(ret);
    }

    public static String camelCase(char[] name)
    {
        StringBuilder sb = new StringBuilder();
        boolean nextUpper = false;
        for (char srcChar : name)
        {
            char dstChar = srcChar;
            if (srcChar == '_')
            {
                nextUpper = true;
            }
            else
            {
                if (nextUpper)
                {
                    dstChar = Character.toUpperCase(srcChar);
                }
                sb.append(dstChar);
                nextUpper = false;
            }
        }

        return sb.toString();
    }

    private void generateColumnClass(
        String[][] fields,
        String[][] fieldsWithSetter
    )
    {
        appendLine("public static class %s implements %s", COLUMN_HOLDER_NAME, COLUMN_INTERFACE_NAME);
        try (IndentLevel clazzIndent = new IndentLevel())
        {
            generateFieldsAndConstructor(fields, fieldsWithSetter);
        }
    }

    private void generateFieldsAndConstructor(
        String[][] fieldInitByConstr,
        String[][] fieldsWithPrivateSetter
    )
    {
        for (String[] field : fieldInitByConstr)
        {
            appendLine("private final %s %s;", field[0], field[1]);
        }
        for (String[] field : fieldsWithPrivateSetter)
        {
            appendLine("private @Nullable %s %s;", field[0], field[1]);
        }
        appendEmptyLine();
        appendLine("public %s(", COLUMN_HOLDER_NAME);
        try (IndentLevel paramIndent = new IndentLevel("", ")", false, true))
        {
            for (String[] field : fieldInitByConstr)
            {
                appendLine("final %s %sRef,", field[0], field[1]);
            }
            cutLastAndAppend(2, "\n");
        }
        try (IndentLevel tableConstructor = new IndentLevel())
        {
            for (String[] field : fieldInitByConstr)
            {
                appendLine("%s = %sRef;", field[1], field[1]);
            }
        }

        for (String[] field : fieldInitByConstr)
        {
            appendEmptyLine();
            appendLine("@Override");
            if (field[1].matches("is[A-Z0-9].*"))
            {
                appendLine("public %s %s()", field[0], field[1]);
            }
            else
            {
                appendLine("public %s get%s()", field[0], firstToUpper(field[1]));
            }
            try (IndentLevel getter = new IndentLevel())
            {
                appendLine("return %s;", field[1]);
            }
        }
        for (String[] field : fieldsWithPrivateSetter)
        {
            appendEmptyLine();
            appendLine("@Override");
            if (field[1].matches("is[A-Z0-9].*"))
            {
                appendLine("public %s %s()", field[0], field[1]);
            }
            else
            {
                appendLine("public %s get%s()", field[0], firstToUpper(field[1]));
            }
            try (IndentLevel getter = new IndentLevel())
            {
                appendLine("return %s;", field[1]);
            }
        }

        appendEmptyLine();
        appendLine("@Override");
        appendLine("public String toString()");
        try (IndentLevel toString = new IndentLevel())
        {
            appendLine("return (table == null ? \"No table set\" : table) + \", Column: \" + name;");
        }
    }

    private StringBuilder appendEmptyLine()
    {
        return mainBuilder.append("\n");
    }

    private StringBuilder appendLine(String format, Object... args)
    {
        return append(format + "%n", args);
    }

    private StringBuilder append(String format, Object... args)
    {
        StringBuilder ret;
        if (format.isBlank())
        {
            ret = appendEmptyLine();
        }
        else
        {
            for (int indent = 0; indent < indentLevel; ++indent)
            {
                mainBuilder.append(INDENT);
            }
            ret = mainBuilder.append(String.format(format, args));
        }
        return ret;
    }

    private class IndentLevel implements AutoCloseable
    {
        private final String closeStr;
        private final boolean indentClose;

        IndentLevel()
        {
            this("{", "}", true, true);
        }

        IndentLevel(
            String openStrRef,
            String closeStrRef,
            boolean indentOpen,
            boolean indentCloseRef
        )
        {
            closeStr = closeStrRef;
            indentClose = indentCloseRef;
            if (indentOpen)
            {
                appendLine(openStrRef);
            }
            else
            {
                mainBuilder.append(openStrRef);
            }
            ++indentLevel;
        }

        @Override
        public void close()
        {
            --indentLevel;
            if (indentClose)
            {
                appendLine(closeStr);
            }
            else
            {
                mainBuilder.append(closeStr);
            }
        }
    }

    public static class Table
    {
        private String name;
        private List<Column> columns = new ArrayList<>();

        private Table(String nameRef)
        {
            name = nameRef;
        }

        public String getName()
        {
            return name;
        }

        public List<Column> getColumns()
        {
            return Collections.unmodifiableList(columns);
        }
    }

    public static class Column
    {
        private Table tbl;
        private String name;
        private String sqlType;
        private boolean pk;
        private boolean nullable;

        private Column(Table tblRef, String colNameRef, String sqlColumnTypeRef, boolean isPkRef, boolean isNullableRef)
        {
            tbl = tblRef;
            name = colNameRef;
            sqlType = sqlColumnTypeRef;
            pk = isPkRef;
            nullable = isNullableRef;
        }

        public Table getTable()
        {
            return tbl;
        }

        public String getName()
        {
            return name;
        }

        public String getSqlType()
        {
            return sqlType;
        }

        public boolean isPk()
        {
            return pk;
        }

        public boolean isNullable()
        {
            return nullable;
        }
    }

    public static class GeneratedCrdResult
    {
        public final Set<GeneratedCrdJavaClass> javaClasses;
        public final Set<GeneratedResources> resources;

        private GeneratedCrdResult(
            Set<GeneratedCrdJavaClass> javaClassesRef,
            Set<GeneratedResources> resourcesRef
        )
        {
            javaClasses = Collections.unmodifiableSet(javaClassesRef);
            resources = Collections.unmodifiableSet(resourcesRef);
        }
    }

    public static class GeneratedCrdJavaClass
    {
        public final String pkgName;
        public final String clazzName;
        public final String javaCode;

        public GeneratedCrdJavaClass(String pkgNameRef, String clazzNameRef, String javaCodeRef)
        {
            pkgName = pkgNameRef;
            clazzName = clazzNameRef;
            javaCode = javaCodeRef;
        }
    }

    public static class GeneratedResources
    {
        public final String yamlLocation;
        public final String content;

        public GeneratedResources(String yamlLocationRef, String contentRef)
        {
            yamlLocation = yamlLocationRef;
            content = contentRef;
        }
    }

    public static class GeneratorVersion implements Comparable<GeneratorVersion>
    {
        public final Version parsedVersion;
        public final String originalVersion;

        public GeneratorVersion(Version parsedVersionRef, String originalVersionRef)
        {
            parsedVersion = parsedVersionRef;
            originalVersion = originalVersionRef;
        }

        @Override
        public int compareTo(GeneratorVersion oRef)
        {
            int cmp = parsedVersion.compareTo(oRef.parsedVersion);
            if (cmp == 0)
            {
                cmp = originalVersion.compareTo(oRef.originalVersion);
            }
            return cmp;
        }

        @Override
        public String toString()
        {
            return originalVersion;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + originalVersion.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            GeneratorVersion other = (GeneratorVersion) obj;
            return Objects.equals(originalVersion, other.originalVersion);
        }
    }
}
