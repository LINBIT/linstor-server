package com.linbit.drbdmanage.propscon;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.linbit.TransactionMgr;

public class DerbyDriverSerialPropsConTest extends DerbyDriverPropsConBase
{
    @Test
    public void test() throws SQLException, InvalidKeyException, InvalidValueException
    {
        SerialPropsContainer container = SerialPropsContainer.createRootContainer(dbDriver);
        String expectedKey = "key";
        String expectedValue = "value";
        String expectedOtherValue = "otherValue";
        container.setProp(expectedKey, expectedValue);

        Map<String, String> expectedMap = new HashMap<>();
        expectedMap.put(expectedKey, expectedValue);
        expectedMap.put(SerialGenerator.KEY_SERIAL, "1");

        checkExpectedMap(expectedMap, container);

        container.closeGeneration();
        container.setProp(expectedKey, expectedOtherValue);
        expectedMap.put(expectedKey, expectedOtherValue);
        expectedMap.put(SerialGenerator.KEY_SERIAL, "2");

        checkExpectedMap(expectedMap, container);

        SerialPropsContainer container2 = SerialPropsContainer.loadContainer(dbDriver, new TransactionMgr(dbConnPool));
        checkExpectedMap(expectedMap, container2);
    }

}
