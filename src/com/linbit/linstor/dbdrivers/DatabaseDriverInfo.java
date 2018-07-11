package com.linbit.linstor.dbdrivers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public interface DatabaseDriverInfo
{
    static void loadDriver(final String driverClass)
    {
        try
        {
            Class<?> loadedClass = Class.forName(driverClass);
            Constructor<?> objConstr = loadedClass.getConstructor();
            objConstr.newInstance();
        }
        catch (final ClassNotFoundException | IllegalAccessException | InstantiationException |
               NoSuchMethodException | IllegalArgumentException | InvocationTargetException loadExc)
        {
            // FIXME: This function should probably have the ability to generate error reports
            //        using LINSTOR's ErrorReporter, so that if the database driver cannot be
            //        loaded, at least it will tell the system's administrator exactly
            //        what went wrong
            String detailsMsg = "Details:\n    ";
            String errMsg = loadExc.getMessage();
            if (errMsg != null)
            {
                detailsMsg += "The runtime environment provided the following error description:\n    " + errMsg;
            }
            else
            {
                detailsMsg += "The runtime environment did not provide additional " +
                    "details about the cause of the error.";
            }
            System.err.println(
                "Loading the database driver class '" + driverClass + "' failed." +
                detailsMsg
            );
        }
    }

    static DatabaseDriverInfo createDriverInfo(final String dbType)
    {
        DatabaseDriverInfo dbdriver = null;
        switch (dbType)
        {
            case "h2":
                dbdriver = new H2DatabaseInfo();
                break;
            case "derby":
                dbdriver = new DerbyDatabaseInfo();
                break;
            case "postgresql":
                dbdriver = new PostgresqlDatabaseInfo();
                break;
            default:
                break;
        }

        return dbdriver;
    }

    String jdbcUrl(String dbPath);
    String jdbcInMemoryUrl();

    String isolationStatement();

    String prepareInit(String initSQL);
}
