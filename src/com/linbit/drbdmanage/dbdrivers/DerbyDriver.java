package com.linbit.drbdmanage.dbdrivers;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DerbyDriver implements DatabaseDriver
{
    public static String DB_CONNECTION_URL = "jdbc:derby:directory:database";

    @Override
    public String getDefaultConnectionUrl()
    {
        return DB_CONNECTION_URL;
    }
}
