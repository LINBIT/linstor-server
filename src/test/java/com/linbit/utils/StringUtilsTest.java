package com.linbit.utils;

import org.junit.Test;
import org.junit.Assert;

public class StringUtilsTest
{
    @Test
    public void testShellQuote()
    {
        Assert.assertEquals("Unexpected quote result", "''", ShellUtils.shellQuote(""));
        Assert.assertEquals("Unexpected quote result", "safe", ShellUtils.shellQuote("safe"));
        Assert.assertEquals("Unexpected quote result", "'un safe'", ShellUtils.shellQuote("un safe"));
        Assert.assertEquals("Unexpected quote result", "'un'\"'\"' '\"'\"'safe'", ShellUtils.shellQuote("un' 'safe"));
        Assert.assertEquals("Unexpected quote result", "'un\" \"safe'", ShellUtils.shellQuote("un\" \"safe"));
    }

    @Test
    public void testJoinShellQuote()
    {
        Assert.assertEquals(
            "Unexpected quote join result",
            "lvs --config '{ }' --empty ''",
            ShellUtils.joinShellQuote("lvs", "--config", "{ }", "--empty", "")
        );
    }
}
