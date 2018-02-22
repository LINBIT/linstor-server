package com.linbit.linstor.api.prop;

import static org.junit.Assert.assertTrue;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.PropsWhitelist.LinStorObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WhitelistTest
{
    private static EmptyErrorReporter errorReporter;
    private PropsWhitelist pwl;

    @BeforeClass
    public static void setUpClass()
    {
        errorReporter = new EmptyErrorReporter();
    }

    @Before
    public void setUp()
    {
        pwl = new PropsWhitelist(errorReporter);
    }

    @Test
    public void storPoolDriverVGNameValidTests()
    {
        assertTrue(pwl.isAllowed(
            LinStorObject.STORAGEPOOL,
            buildKey(ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_VOLUME_GROUP),
            "validName")
        );
    }

    @Test(expected = ImplementationError.class)
    public void storPoolDriverVGNameInvalidTests()
    {
        pwl.isAllowed(
            LinStorObject.STORAGEPOOL,
            buildKey(ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_VOLUME_GROUP),
            "in_validName"
        );
        // rule should be internal, thus throw an exception when the client sent us invalid data
    }


    private String buildKey(String... parts)
    {
        return Arrays.asList(parts).stream().collect(Collectors.joining(Props.PATH_SEPARATOR));
    }
}
