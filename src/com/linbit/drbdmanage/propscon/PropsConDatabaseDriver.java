package com.linbit.drbdmanage.propscon;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface PropsConDatabaseDriver
{
    void persist(Map<String, String> props) throws SQLException;

    Map<String, String> load() throws SQLException;

    void persist(String key, String value) throws SQLException;

    void remove(String key) throws SQLException;

    void remove(Set<String> keys) throws SQLException;

    void removeAll() throws SQLException;

    String getInstanceName();
}
