/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.linbit.linstor.dbdrivers;

/**
 * Database driver information for IBM DB2
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Db2DatabaseInfo implements DatabaseDriverInfo
{
    @Override
    public String jdbcUrl(String dbPath)
    {
        return "jdbc:db2:" + dbPath;
    }

    @Override
    public String jdbcInMemoryUrl()
    {
        return null;
    }

    @Override
    public String isolationStatement()
    {
        return "SET ISOLATION SERIALIZABLE;";
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL;
    }
}
