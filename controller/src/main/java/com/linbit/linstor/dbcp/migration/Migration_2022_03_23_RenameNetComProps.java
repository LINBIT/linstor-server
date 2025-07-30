package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;

@Migration(
    version = "2022.03.23.09.00",
    description = "Rename netcom props to NetCom"
)
public class Migration_2022_03_23_RenameNetComProps extends LinstorMigration
{
    public static final String TBL_PROPS_CON = "PROPS_CONTAINERS";
    public static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    public static final String PROP_KEY = "PROP_KEY";
    public static final String PROP_VALUE = "PROP_VALUE";

    public static final String NETCOM_NAMESPACE_OLD = "netcom";
    public static final String NETCOM_NAMESPACE_NEW = "NetCom";

    public static final HashMap<String, String> KEY_RENAME_MAP = new HashMap<>();
    public static final HashMap<String, String> TYPE_VALUE_RENAME_MAP = new HashMap<>();

    static
    {
        KEY_RENAME_MAP.put("bindaddress", "BindAddress");
        KEY_RENAME_MAP.put("enabled", "Enabled");
        KEY_RENAME_MAP.put("keyPasswd", "KeyPasswd");
        KEY_RENAME_MAP.put("keyStore", "KeyStore");
        KEY_RENAME_MAP.put("keyStorePasswd", "KeyStorePasswd");
        KEY_RENAME_MAP.put("port", "Port");
        KEY_RENAME_MAP.put("sslProtocol", "SslProtocol");
        KEY_RENAME_MAP.put("trustStore", "TrustStore");
        KEY_RENAME_MAP.put("trustStorePasswd", "TrustStorePasswd");
        KEY_RENAME_MAP.put("type", "Type");

        TYPE_VALUE_RENAME_MAP.put("ssl", "SSL");
        TYPE_VALUE_RENAME_MAP.put("plain", "Plain");
    }

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        try (
            ResultSet resultSet = connection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE
            )
                .executeQuery(
                    "SELECT " + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE + " FROM " + TBL_PROPS_CON +
                        " WHERE " + PROP_KEY + " LIKE '" + NETCOM_NAMESPACE_OLD + "%'"
            );
        )
        {
            while (resultSet.next())
            {
                String oldKey = resultSet.getString(PROP_KEY);
                String newKey = getNewKey(oldKey);

                resultSet.updateString(PROP_KEY, newKey);

                String oldValue = resultSet.getString(PROP_VALUE);
                String newValue = getNewValue(oldKey, oldValue);
                if (!oldValue.equals(newValue))
                {
                    resultSet.updateString(PROP_VALUE, newValue);
                }

                resultSet.updateRow();
            }
        }
    }

    public static String getNewKey(String oldKey)
    {
        String[] parts = oldKey.split("/");
        String keyNew = NETCOM_NAMESPACE_NEW + "/" + parts[1] + "/" + KEY_RENAME_MAP.get(parts[2]);
        return keyNew;
    }

    public static String getNewValue(String oldKey, String oldValue)
    {
        String[] parts = oldKey.split("/");
        String value = oldValue;
        if ("type".equals(parts[2]))
        {
            value = TYPE_VALUE_RENAME_MAP.get(oldValue);
        }
        return value;
    }
}
