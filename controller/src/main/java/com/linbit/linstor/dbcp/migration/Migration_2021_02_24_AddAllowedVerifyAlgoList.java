package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2021.02.24.12.00",
    description = "Add allowed verify algorithm list"
)
public class Migration_2021_02_24_AddAllowedVerifyAlgoList extends LinstorMigration
{
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    private static final String PROP_KEY = "PROP_KEY";
    private static final String PROP_VALUE = "PROP_VALUE";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        final String propKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/auto-verify-algo-allowed-list";
        SQLUtils.runSql(
            connection,
            " INSERT INTO " + TBL_PROPS_CONTAINERS +
                " (" + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE + ") " +
                " VALUES ('/CTRLCFG', '" + propKey + "', 'crct10dif-pclmul;crc32-pclmul;crc32c-intel;" +
                "crct10dif-generic;crc32c-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic')"
        );
    }
}
