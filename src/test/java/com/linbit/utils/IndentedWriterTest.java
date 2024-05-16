package com.linbit.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class IndentedWriterTest
{
    private IndentedWriter iw;
    private StringBuilder expectedOut;

    @Before
    public void setup()
    {
        iw = new IndentedWriter();
        expectedOut = new StringBuilder();
    }

    @Test
    public void withIndentGlobalTest()
    {
        iw.withIndentGlobal("__").appendIfExists("descr1", "test");
        expectAdditional("__descr1: test\n");

        iw.withIndentGlobal("+").appendIfExists("descr2", "test2");
        expectAdditional("+descr2: test2\n");
    }

    @Test
    public void withIndentNewLineTest()
    {
        iw.withIndentNewLine("_").appendIfExists("descr1", "test");
        expectAdditional("descr1: test\n");

        iw.appendIfNotEmpty("descr2", Arrays.asList("1", "2"));
        expectAdditional(
            "descr2s:\n" +
            "_1\n" +
            "_2\n");
    }

    @Test
    public void appendIfExistsTest()
    {
        expectAdditional("");

        iw.appendIfExists("descr1", "bla");
        expectAdditional("descr1: bla\n");

        iw.appendIfExists("descr2", null);
        expectAdditional("");

        iw.appendIfExists("descr3", null, "suffix");
        expectAdditional("");

        iw.appendIfExists("descr4", "elem", "suffix");
        expectAdditional("descr4elemsuffix");
    }

    @Test
    public void appendIfNotEmptyCollectionTest()
    {
        iw.appendIfNotEmpty("descr1", Arrays.asList(1));
        expectAdditional(
            "descr1:\n" +
            "   1\n"
        );

        iw.appendIfNotEmpty("descr2", Arrays.asList());
        expectAdditional("");

        iw.appendIfNotEmpty("descr3", (Collection<Object>) null);
        expectAdditional("");
    }

    @Test
    public void appendIfNotEmptyCollectionPluralTest()
    {
        iw.appendIfNotEmpty("descr1", Arrays.asList(1));
        expectAdditional("descr1:\n" +
            "   1\n"
        );

        iw.appendIfNotEmpty("descr2", Arrays.asList(1, 2));
        expectAdditional(
            "descr2s:\n" +
            "   1\n"+
            "   2\n"
        );

        iw.appendIfNotEmpty("descr3", "descr3plural", Arrays.asList(1));
        expectAdditional(
            "descr3:\n" +
            "   1\n"
        );

        iw.appendIfNotEmpty("descr4", "descr4plural", Arrays.asList(1, 2));
        expectAdditional(
            "descr4plural:\n" +
            "   1\n" +
            "   2\n"
        );
    }

    @Test
    public void appendIfNotEmptyMapTest()
    {
        iw.appendIfNotEmpty("descr1", Map.of("a", "b"));
        expectAdditional(
            "descr1:\n" +
                "   a: b\n"
        );

        iw.appendIfNotEmpty("descr2", Map.of());
        expectAdditional("");

        iw.appendIfNotEmpty("descr3", (Map<Object, Object>) null);
        expectAdditional("");
    }

    @Test
    public void appendIfNotEmptyMapPluralTest()
    {
        Map<String, String> singularMap = new LinkedHashMap<>();
        singularMap.put("a", "1");
        Map<String, String> pluralMap = new LinkedHashMap<>();
        pluralMap.put("a", "1");
        pluralMap.put("b", "2");

        String expectedSingularStr = "   a: 1\n";
        String expectedPluralStr = "   a: 1\n   b: 2\n";

        iw.appendIfNotEmpty("descr1", singularMap);
        expectAdditional("descr1:\n" + expectedSingularStr);

        iw.appendIfNotEmpty("descr2", pluralMap);
        expectAdditional("descr2s:\n" + expectedPluralStr);

        iw.appendIfNotEmpty("descr3", "descr3plural", singularMap);
        expectAdditional("descr3:\n" + expectedSingularStr);

        iw.appendIfNotEmpty("descr4", "descr4plural", pluralMap);
        expectAdditional("descr4plural:\n" + expectedPluralStr);
    }

    @Test
    public void appendIfNotEmptyMapNoPluralTest()
    {
        Map<String, String> singularMap = new LinkedHashMap<>();
        singularMap.put("a", "1");
        Map<String, String> pluralMap = new LinkedHashMap<>();
        pluralMap.put("a", "1");
        pluralMap.put("b", "2");

        String expectedSingularStr = "   a: 1\n";
        String expectedPluralStr = "   a: 1\n   b: 2\n";

        iw.appendIfNotEmptyNoPlural("descr1", singularMap);
        expectAdditional("descr1:\n" + expectedSingularStr);

        iw.appendIfNotEmptyNoPlural("descr2", pluralMap);
        expectAdditional("descr2:\n" + expectedPluralStr);
    }

    private void expectAdditional(String additionalStr)
    {
        expectedOut.append(additionalStr);
        assertThat(iw).hasToString(expectedOut.toString());
    }
}
