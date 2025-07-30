package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.07.23.10.00",
    description = "Disable Plain and SSL Connector"
)
public class Migration_2020_07_23_Disable_PlainSSLConnector extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        SQLUtils.runSql(
            connection,
            "UPDATE PROPS_CONTAINERS " +
                " SET PROP_VALUE = '' " +
                " WHERE PROPS_INSTANCE = '/CTRLCFG' AND" +
                " PROP_KEY IN ('netcom/PlainConnector/bindaddress', 'netcom/SslConnector/bindaddress')"

        );
    }
}
