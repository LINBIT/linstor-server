package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.util.UUID;

@Migration(
    version = "2021.06.25.09.00",
    description = "Generate ID for local linstor cluster"
)
public class Migration_2021_06_25_GenerateClusterId extends LinstorMigration
{
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    private static final String PROP_KEY = "PROP_KEY";
    private static final String PROP_VALUE = "PROP_VALUE";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        String propKey = "Cluster/LocalID";
        String clusterId = UUID.randomUUID().toString();
        SQLUtils.runSql(
            connection,
            " INSERT INTO " + TBL_PROPS_CONTAINERS +
                " (" + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE + ") " +
                " VALUES ('/CTRLCFG', '" + propKey + "', '" + clusterId + "')"
        );
        SQLUtils.runSql(
            connection,
            " INSERT INTO " + TBL_PROPS_CONTAINERS +
                " (" + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE + ") " +
                " VALUES ('STLTCFG', '" + propKey + "', '" + clusterId + "')"
        );
    }
}
