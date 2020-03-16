package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public final class DatabaseConstantsGenerator
{
    public static final String DFLT_PACKAGE = "com.linbit.linstor.dbdrivers";
    public static final String DFLT_CLAZZ_NAME = "GeneratedDatabaseTables";

    public static final String[] IMPORTS = new String[]
    {
        "com.linbit.linstor.dbdrivers.DatabaseTable",
        "com.linbit.linstor.dbdrivers.DatabaseTable.Column",
        null, // empty line
        "java.sql.Types"
    };

    private static final String INTERFACE_NAME = "DatabaseTable";
    private static final String COLUMN_HOLDER_NAME = "ColumnImpl";
    private static final String COLUMN_INTERFACE_NAME = "Column";
    private static final String INDENT = "    ";
    private static final String DB_SCHEMA = "LINSTOR";
    private static final String TYPE_TABLE = "TABLE";

    private static final List<String> IGNORED_TABLES = Arrays.asList(
        "FLYWAY_SCHEMA_HISTORY"
    );

    private StringBuilder clazzBuilder;
    private int indentLevel = 0;
    private TreeMap<String, Table> tbls = new TreeMap<>();

    public static String generateSqlConstants(Connection conRef)
    {
        return generateSqlConstants(conRef, null, null);
    }

    public static String generateSqlConstants(Connection conRef, String pkgStrRef, String clazzNameRef)
    {
        DatabaseConstantsGenerator generator = new DatabaseConstantsGenerator();
        String generatedClass;
        try
        {
            String pkgStr = pkgStrRef == null ? DFLT_PACKAGE : pkgStrRef;
            String clazzName = clazzNameRef == null ? DFLT_CLAZZ_NAME : clazzNameRef;

            generatedClass = generator.generate(conRef, pkgStr, clazzName);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(exc);
        }
        return generatedClass;
    }


    private String generate(Connection con, String pkgName, String clazzName) throws Exception
    {
        extractTables(con);
        String generatedTablesJavaClassSrc = render(pkgName, clazzName);

        return generatedTablesJavaClassSrc;
    }

    private void extractTables(Connection con) throws SQLException
    {
        try
        (
            ResultSet tables = con.getMetaData().getTables(
                null,
                DB_SCHEMA,
                null,
                new String[] {TYPE_TABLE}
            )
        )
        {
            while (tables.next())
            {
                String tblName = tables.getString("TABLE_NAME");
                if (!IGNORED_TABLES.contains(tblName))
                {
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
                        ResultSet columns = con.getMetaData().getColumns(
                            null,
                            DB_SCHEMA,
                            tblName,
                            null
                        )
                    )
                    {
                        while (columns.next())
                        {
                            String clmName = columns.getString("COLUMN_NAME");
                            tbl.columns.add(
                                new Column(
                                    clmName,
                                    columns.getString("TYPE_NAME"),
                                    primaryKeys.contains(clmName),
                                    columns.getString("IS_NULLABLE").equalsIgnoreCase("yes")
                                )
                            );
                        }
                    }
                    tbls.put(tbl.name, tbl);
                }
            }
        }
    }

    private String render(String pkgName, String clazzName) throws Exception
    {
        clazzBuilder = new StringBuilder();

        renderPackageAndImports(pkgName);

        appendEmptyLine();
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
                renderTable(tbl);
                appendEmptyLine();
            }

            // table constants
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
                new String[][] {
                    {"String", "name"},
                    {"int", "sqlType"},
                    {"boolean", "isPk"},
                    {"boolean", "isNullable"}
                },
                new String[][] {
                    {INTERFACE_NAME, "table"}
                }
            );

        }
        return clazzBuilder.toString();
    }

    private void renderPackageAndImports(String pkgName)
    {
        appendLine("package %s;", pkgName);
        appendEmptyLine();
        for (String imp : IMPORTS)
        {
            if (imp == null)
            {
                appendEmptyLine();
            }
            else
            {
                appendLine("import " + imp + ";");
            }
        }
    }

    private void renderTable(Table tbl)
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

        appendLine("public static class %s implements %s", tblNameCamelCase, INTERFACE_NAME);
        try (IndentLevel tblDfn = new IndentLevel())
        {
            // constructor
            appendLine("private %s() { }", tblNameCamelCase);
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

    private void cutLastAndAppend(int cutLen, String appendAfter)
    {
        clazzBuilder.setLength(clazzBuilder.length() - cutLen);
        clazzBuilder.append(appendAfter);
    }


    private String toUpperCamelCase(String nameRef)
    {
        return camelCase(firstToUpper(nameRef.toLowerCase()).toCharArray());
    }

    private String firstToUpper(String nameRef)
    {
        char[] ret = nameRef.toCharArray();
        ret[0] = Character.toUpperCase(ret[0]);
        return new String(ret);
    }

    private String camelCase(char[] name)
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
        throws Exception
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
            appendLine("private %s %s;", field[0], field[1]);
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
            appendLine("return (table == null ? \"No table set\" : table ) + \", Column: \" + name;");
        }
    }

    private void appendEmptyLine()
    {
        clazzBuilder.append("\n");
    }

    private void appendLine(String format, Object... args)
    {
        append(format, args).append("\n");
    }

    private StringBuilder append(String format, Object... args)
    {
        for (int indent = 0; indent < indentLevel; ++indent)
        {
            clazzBuilder.append(INDENT);
        }
        return clazzBuilder.append(String.format(format, args));
    }

    private DatabaseConstantsGenerator()
    {
    }

    private class IndentLevel implements AutoCloseable
    {
        private final String closeStr;
        private final boolean indentClose;

        IndentLevel()
        {
            this ("{", "}", true, true);
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
                clazzBuilder.append(openStrRef);
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
                clazzBuilder.append(closeStr);
            }
        }
    }

    private class Table
    {
        private String name;
        private List<Column> columns = new ArrayList<>();

        public Table(String nameRef)
        {
            name = nameRef;
        }
    }

    private class Column
    {
        private String name;
        private String sqlType;
        private boolean pk;
        private boolean nullable;

        public Column(String colNameRef, String sqlColumnTypeRef, boolean isPkRef, boolean isNullableRef)
        {
            name = colNameRef;
            sqlType = sqlColumnTypeRef;
            pk = isPkRef;
            nullable = isNullableRef;
        }
    }
}
