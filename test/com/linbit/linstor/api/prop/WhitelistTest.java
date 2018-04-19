package com.linbit.linstor.api.prop;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.AbsApiCallHandler.LinStorObject;
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
    private WhitelistProps pwl;

    @BeforeClass
    public static void setUpClass()
    {
        errorReporter = new EmptyErrorReporter();
    }

    @Before
    public void setUp()
    {
        pwl = new WhitelistProps(errorReporter);
    }

    @Test
    public void storPoolDriverVGNameValidTests()
    {
        assertTrue(pwl.isAllowed(
            LinStorObject.STORAGEPOOL,
            buildKey(ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_VOLUME_GROUP),
            "validName", false)
        );
    }

    @Test
    public void storPoolDriverVGNameInvalidTests()
    {
        assertFalse(
            pwl.isAllowed(
                LinStorObject.STORAGEPOOL,
                buildKey(ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_VOLUME_GROUP),
                "in_validName+*", false
            )
        );
    }

    @Test
    public void numericOrSymbolTest()
    {
        assertTrue(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/quorum",
                "3", false
            )
        );
        assertTrue(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/quorum",
                "32", false
            )
        );
        assertTrue(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/quorum",
                "off", false
            )
        );

        assertFalse(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/quorum",
                "33", false
            )
        );
        assertFalse(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/quorum",
                "no", false
            )
        );
    }

    @Test
    public void rangeTest()
    {
        assertTrue(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/twopc-retry-timeout",
                "1", false
            )
        );
        assertTrue(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/twopc-retry-timeout",
                "50", false
            )
        );
        assertFalse(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/twopc-retry-timeout",
                "0", false
            )
        );
        assertFalse(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Resource/twopc-retry-timeout",
                "51", false
            )
        );
    }

    @Test
    public void symbolTest()
    {
        assertTrue(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Net/after-sb-0pri",
                "disconnect", false
            )
        );
        assertFalse(
            pwl.isAllowed(
                LinStorObject.RESOURCE_DEFINITION,
                "DrbdOptions/Net/after-sb-0pri",
                "invalid", false
            )
        );
    }

    private String buildKey(String... parts)
    {
        return Arrays.asList(parts).stream().collect(Collectors.joining(Props.PATH_SEPARATOR));
    }
}
