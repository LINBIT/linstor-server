package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

public final class DatabaseConstantsGenerator
{
    public static final String DFLT_PACKAGE = "com.linbit.linstor.dbdrivers";
    public static final String DFLT_CLAZZ_NAME = "GeneratedDatabaseTables";

    private static final String INTERFACE_NAME = "Table";
    private static final String COLUMN_HOLDER_NAME = "Column";
    private static final String INDENT = "    ";
    private static final String DB_SCHEMA = "LINSTOR";
    private static final String TYPE_TABLE = "TABLE";

    private static final List<String> IGNORED_TABLES = Arrays.asList(
        "FLYWAY_SCHEMA_HISTORY"
    );

    private final StringBuilder mainBuilder = new StringBuilder();
    private final StringBuilder tableInstanceBuilder = new StringBuilder();
    private final StringBuilder tableStaticInitializerBuilder = new StringBuilder();
    private final Stack<StringBuilder> activeBuilder = new Stack<>();
    private final TreeSet<String> tableNames = new TreeSet<>();
    private final TreeSet<String> columnNames = new TreeSet<>();
    private int indentLevel = 0;

    public static String generateSqlConstants(Connection conRef)
    {
        return generateSqlConstants(conRef, null, null);
    }

    public static String generateSqlConstants(Connection conRef, String pkgStrRef, String clazzNameRef)
    {
        DatabaseConstantsGenerator generator = new DatabaseConstantsGenerator();
        try
        {
            String pkgStr = pkgStrRef == null ? DFLT_PACKAGE : pkgStrRef;
            String clazzName = clazzNameRef == null ? DFLT_CLAZZ_NAME : clazzNameRef;

            generator.generate(conRef, pkgStr, clazzName);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(exc);
        }
        return generator.mainBuilder.toString();
    }

    private void generate(Connection con, String pkgName, String clazzName) throws Exception
    {
        activeBuilder.add(mainBuilder);

        appendLine("package %s;\n", pkgName);
        appendLine("import java.sql.Types;");
        appendEmptyLine();
        appendLine("public class %s", clazzName);
        try (IndentLevel clazzIndent = new IndentLevel())
        {
            appendLine("/**");
            appendLine(" * Marker interface. All following tables share this interface");
            appendLine(" */");
            appendLine("public interface " + INTERFACE_NAME);
            try (IndentLevel iface = new IndentLevel())
            {
                appendLine("/**");
                appendLine(" * Returns all columns of the current table");
                appendLine(" */");
                appendLine("Column[] values();");
                appendEmptyLine();
                appendLine("/**");
                appendLine(" * Returns the name of the current table");
                appendLine(" */");
                appendLine("String getName();");
            }
            appendEmptyLine();
            appendLine("private %s()", clazzName);
            try (IndentLevel constructor = new IndentLevel())
            {
            }
            appendEmptyLine();

            appendLine("// Schema name");
            appendLine("public static final String DATABASE_SCHEMA_NAME = \"%s\";", DB_SCHEMA);

            try(ResultSet tables = con.getMetaData().getTables(
                null,
                DB_SCHEMA,
                null,
                new String[] { TYPE_TABLE }
            ))
            {
                while (tables.next())
                {
                    final String tableName = tables.getString("TABLE_NAME");
                    if (!IGNORED_TABLES.contains(tableName))
                    {
                        generateTable(clazzName, con, tableName);
                        tableNames.add(tableName);
                    }
                    appendEmptyLine();
                }
            }

            activeBuilder.peek().append(tableInstanceBuilder);

            appendEmptyLine();
            appendLine("static");
            try (IndentLevel staticBlock = new IndentLevel())
            {
                activeBuilder.peek().append(tableStaticInitializerBuilder);
            }

            appendEmptyLine();
            generateHolderClass(COLUMN_HOLDER_NAME, new String[][] {
                {"int", "index"},
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
    }

    private void cutLastAndAppend(int cutLen, String appendAfter)
    {
        StringBuilder sb = activeBuilder.peek();
        sb.setLength(sb.length() - cutLen);
        sb.append(appendAfter);
    }

    private void generateTable(String outerTableName, Connection con, String tableName) throws Exception
    {
        Set<String> primaryKeys = new TreeSet<>();
        try (ResultSet primaryKeyResultSet = con.getMetaData().getPrimaryKeys(null, DB_SCHEMA, tableName))
        {
            while (primaryKeyResultSet.next())
            {
                primaryKeys.add(primaryKeyResultSet.getString("COLUMN_NAME"));
            }
        }

        String tableClassName = toUpperCamelCase(tableName);
        appendLine("public static class %s implements %s", tableClassName, INTERFACE_NAME);
        try (IndentLevel tblDfn = new IndentLevel())
        {
            appendLine("private %s() { }", tableClassName);
            appendEmptyLine();
            try (ResultSet columns = con.getMetaData().getColumns(
                null,
                DB_SCHEMA,
                tableName,
                null
            ))
            {
                ArrayList<String> currentColumnNames = new ArrayList<>();
                int index = 0;
                while (columns.next())
                {
                    final String colName = columns.getString("COLUMN_NAME");
                    appendLine(
                        "public static final Column %s = new Column(%d, \"%s\", Types.%s, %s, %s);",
                            colName,
                            index,
                            colName,
                            columns.getString("TYPE_NAME"),
                            Boolean.toString(primaryKeys.contains(colName)),
                            Boolean.toString(columns.getString("IS_NULLABLE").equalsIgnoreCase("yes"))
                        );
                    ++index;
                    columnNames.add(colName);
                    currentColumnNames.add(colName);

                    activeBuilder.push(tableStaticInitializerBuilder);
                    appendLine("%s.%s.table = %s;", tableClassName, colName, tableName);
                    activeBuilder.pop();
                }
                appendEmptyLine();
                appendLine("public static final Column[] ALL = new Column[]");
                try (IndentLevel allColumns = new IndentLevel("{", "};", true, true))
                {
                    for (String curColName : currentColumnNames)
                    {
                        appendLine("%s,", curColName);
                    }
                    cutLastAndAppend(2, "\n");
                }
                appendEmptyLine();
                appendLine("@Override");
                appendLine("public Column[] values()");
                try (IndentLevel valuesMethod = new IndentLevel())
                {
                    appendLine("return ALL;");
                }
                appendEmptyLine();
                appendLine("@Override");
                appendLine("public String getName()");
                try (IndentLevel valuesMethod = new IndentLevel())
                {
                    appendLine("return \"%s\";", tableName);
                }
            }
        }

        activeBuilder.add(tableInstanceBuilder);
        appendLine("public static final %s %s = new %s();", tableClassName, tableName, tableClassName);
        activeBuilder.pop();
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

    private String toLowerCamelCase(String nameRef)
    {
        return camelCase(nameRef.toLowerCase().toCharArray());
    }

    private String camelCase(char[] name)
    {
        StringBuilder sb = new StringBuilder();
        boolean nextUpper = false;
        for (char c : name)
        {
            if (c == '_')
            {
                nextUpper = true;
            }
            else
            {
                if (nextUpper)
                {
                    c = Character.toUpperCase(c);
                }
                sb.append(c);
                nextUpper = false;
            }
        }

        return sb.toString();
    }

    private void generateHolderClass(String clazzName, String[][] fields, String[][] fieldsWithSetter) throws Exception
    {
        appendLine("public static class %s", clazzName);
        try (IndentLevel clazzIndent = new IndentLevel())
        {
            generateFieldsAndConstructor(clazzName, fields, fieldsWithSetter);
        }
    }

    private void generateFieldsAndConstructor(
        String clazzName,
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
        appendLine("public %s(", clazzName);
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
    }

    private void appendEmptyLine()
    {
        activeBuilder.peek().append("\n");
    }

    private void appendLine(String format, Object... args)
    {
        append(format, args).append("\n");
    }

    private StringBuilder append(String format, Object... args)
    {
        StringBuilder builder = activeBuilder.peek();
        for (int indent = 0; indent < indentLevel; ++indent)
        {
            builder.append(INDENT);
        }
        return builder.append(String.format(format, args));
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
                activeBuilder.peek().append(openStrRef);
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
                activeBuilder.peek().append(closeStr);
            }
        }
    }
}
