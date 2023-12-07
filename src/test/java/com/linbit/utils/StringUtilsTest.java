package com.linbit.utils;

import org.junit.Test;
import org.junit.Assert;

public class StringUtilsTest
{
    @Test
    public void testShellQuote()
    {
        Assert.assertEquals("Unexpected quote result", "''", StringUtils.shellQuote(""));
        Assert.assertEquals("Unexpected quote result", "safe", StringUtils.shellQuote("safe"));
        Assert.assertEquals("Unexpected quote result", "'un safe'", StringUtils.shellQuote("un safe"));
        Assert.assertEquals("Unexpected quote result", "'un'\"'\"' '\"'\"'safe'", StringUtils.shellQuote("un' 'safe"));
        Assert.assertEquals("Unexpected quote result", "'un\" \"safe'", StringUtils.shellQuote("un\" \"safe"));
    }

    @Test
    public void testJoinShellQuote()
    {
        Assert.assertEquals(
            "Unexpected quote join result",
            "lvs --config '{ }' --empty ''",
            StringUtils.joinShellQuote("lvs", "--config", "{ }", "--empty", "")
        );
    }
}
