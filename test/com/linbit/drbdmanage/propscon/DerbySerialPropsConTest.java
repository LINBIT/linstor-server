package com.linbit.drbdmanage.propscon;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.linbit.TransactionMgr;

public class DerbySerialPropsConTest extends DerbyPropsConBase
{
    @Test
    public void test() throws SQLException, InvalidKeyException, InvalidValueException
    {
        @SuppressWarnings("resource")
        Connection con = getConnection();
        SerialPropsContainer container = SerialPropsContainer.getInstance(dbDriver, null, null);
        TransactionMgr transMgr = new TransactionMgr(con);
        container.setConnection(transMgr);
        String expectedKey = "key";
        String expectedValue = "value";
        String expectedOtherValue = "otherValue";
        container.setProp(expectedKey, expectedValue);
        transMgr.commit();

        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put(expectedKey, expectedValue);
        expectedMap.put(SerialGenerator.KEY_SERIAL, "1");

        checkExpectedMap(expectedMap, container);

        container.closeGeneration();
        container.setProp(expectedKey, expectedOtherValue);
        transMgr.commit();

        expectedMap.put(expectedKey, expectedOtherValue);
        expectedMap.put(SerialGenerator.KEY_SERIAL, "2");

        checkExpectedMap(expectedMap, container);

        SerialPropsContainer container2 = SerialPropsContainer.getInstance(
            dbDriver,
            new TransactionMgr(
                con
            ),
            null
        );
        checkExpectedMap(expectedMap, container2);
    }

}
