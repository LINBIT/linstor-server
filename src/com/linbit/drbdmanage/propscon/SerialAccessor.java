package com.linbit.drbdmanage.propscon;

import java.sql.SQLException;

/**
 * Access interface for serial numbers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SerialAccessor
{
    long getSerial() throws SQLException;
    void setSerial(long serial) throws SQLException;
}
