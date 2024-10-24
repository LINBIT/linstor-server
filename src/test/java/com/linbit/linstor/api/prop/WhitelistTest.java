package com.linbit.linstor.api.prop;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WhitelistTest
{
    private static EmptyErrorReporter errorReporter;
    private static String xmlHeader;
    private static String xmlFooter;

    private PropBuilder builder;

    private static List<String> ignoredKeys = Arrays.asList(ApiConsts.NAMESPC_AUXILIARY + "/");

    @BeforeClass
    public static void setUpClass()
    {
        errorReporter = new EmptyErrorReporter();

        xmlHeader = new StringBuilder().append("<command name=\"test-options\">\n")
            .append("<summary>this is just a test xml</summary>\n")
            .append("<argument>test</argument>\n")
            .append("<option name=\"set-defaults\" type=\"flag\">\n")
            .append("</option>\n").toString();

        xmlFooter = "</command>\n";
    }

    @Before
    public void setUp()
    {
        builder = new PropBuilder();
    }

    @Test
    public void handlerTest()
    {
        WhitelistProps whitelistProps = builder
            .appendHandler("testHandler", "test", "handler", "other-option")
            .build();

        assertValid(whitelistProps, "testHandler", "test");
        assertValid(whitelistProps, "testHandler", "handler");
        assertValid(whitelistProps, "testHandler", "other-option");

        assertInvalid(whitelistProps, "otherHandler", "test");
        assertInvalid(whitelistProps, "testHandler", "test2");
    }


    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void numericTest()
    {
        WhitelistProps whitelistProps = builder
            .appendNumeric("testNumeric", 1, 42)
            .build();

        assertValid(whitelistProps, "testNumeric", "1");
        assertValid(whitelistProps, "testNumeric", "3");
        assertValid(whitelistProps, "testNumeric", "13");
        assertValid(whitelistProps, "testNumeric", "42");

        assertInvalid(whitelistProps, "testNumeric", "0");
        assertInvalid(whitelistProps, "testNumeric2", "3");
        assertInvalid(whitelistProps, "testNumeric", "43");
        assertInvalid(whitelistProps, "testNumeric", "329");
    }

    @Test
    public void booleanTest()
    {
        WhitelistProps whitelistProps = builder
            .appendBooleanTrueFalse("testBoolean")
            .build();

        assertValid(whitelistProps, "testBoolean", "true");
        assertValid(whitelistProps, "testBoolean", "True");
        assertValid(whitelistProps, "testBoolean", "TRUE");
        assertValid(whitelistProps, "testBoolean", "FALSE");
        assertValid(whitelistProps, "testBoolean", "False");
        assertValid(whitelistProps, "testBoolean", "false");
        assertValid(whitelistProps, "testBoolean", "no");
        assertValid(whitelistProps, "testBoolean", "yes");
        assertValid(whitelistProps, "testBoolean", "YES");
        assertValid(whitelistProps, "testBoolean", "NO");

        assertInvalid(whitelistProps, "testBoolean", "sure");
        assertInvalid(whitelistProps, "testBoolean", "nope");
        assertInvalid(whitelistProps, "testBoolean", "1");
        assertInvalid(whitelistProps, "testBoolean", "0");
    }

    @Test
    public void stringTest()
    {
        WhitelistProps whitelistProps = builder
            .appendString("testString")
            .build();

        assertValid(whitelistProps, "testString", "yes");
        assertValid(whitelistProps, "testString", "sure");
        assertValid(whitelistProps, "testString", "boring");
        assertValid(whitelistProps, "testString", "everything");
        assertValid(whitelistProps, "testString", "even spaces");
        assertValid(whitelistProps, "testString", "4nd numb3rs");
        assertValid(whitelistProps, "testString", "and other chars!");
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void numericOrSymbolTest()
    {
        WhitelistProps whitelistProps = builder
            .appendNumericOrSymbol("testNrOrSym", 1, 42, "test", "test2")
            .build();


        assertValid(whitelistProps, "testNrOrSym", "1");
        assertValid(whitelistProps, "testNrOrSym", "3");
        assertValid(whitelistProps, "testNrOrSym", "13");
        assertValid(whitelistProps, "testNrOrSym", "42");

        assertValid(whitelistProps, "testNrOrSym", "test");
        assertValid(whitelistProps, "testNrOrSym", "test2");

        assertInvalid(whitelistProps, "testNrOrSym", "0");
        assertInvalid(whitelistProps, "testNrOrSym", "43");
        assertInvalid(whitelistProps, "otherHandler", "test");
        assertInvalid(whitelistProps, "testNrOrSym", "other-option");
    }

    private void assertValid(WhitelistProps whitelistProps, String key, String value)
    {
        assertTrue(isValid(whitelistProps, LinStorObject.CTRL, key, value));
    }

    private boolean isValid(WhitelistProps whitelistProps, LinStorObject lsObj, String key, String value)
    {
        return whitelistProps.isAllowed(lsObj, ignoredKeys, key, value, false);
    }

    private void assertInvalid(WhitelistProps whitelistProps, String key, String value)
    {
        assertFalse(isValid(whitelistProps, LinStorObject.CTRL, key, value));
    }

    private class PropBuilder
    {
        StringBuilder xmlBuilder = new StringBuilder();
        WhitelistProps whitelistProps;

        PropBuilder()
        {
            this (new WhitelistProps(errorReporter));
        }

        PropBuilder(WhitelistProps whitelistPropsRef)
        {
            whitelistProps = whitelistPropsRef;

            // clear all previously loaded and default rules (i.e. from GeneratedPropertyRules.java)
            whitelistPropsRef.reconfigure(LinStorObject.values());

            xmlBuilder.append(xmlHeader);
        }

        public PropBuilder appendHandler(String optionName, String... handlerOpts)
        {
            appendOptionHeader(optionName, "handler");
            for (String handlerOpt : handlerOpts)
            {
                appendSimpleElement("handler", handlerOpt);
            }
            appendOptionFooter();
            return this;
        }

        public PropBuilder appendNumeric(String optName, int min, int max)
        {
            appendOptionHeader(optName, "numeric");
            appendSimpleElement("min", Integer.toString(min));
            appendSimpleElement("max", Integer.toString(max));
            appendSimpleElement("default", Integer.toString((min + max) / 2)); // not ideal, but enough for tests
            appendOptionFooter();
            return this;
        }

        public PropBuilder appendNumericOrSymbol(String optName, int min, int max, String... values)
        {
            appendOptionHeader(optName, "numeric-or-symbol");
            appendSimpleElement("min", Integer.toString(min));
            appendSimpleElement("max", Integer.toString(max));
            appendSimpleElement("default", Integer.toString((min + max) / 2)); // not ideal, but enough for tests
            for (String value : values)
            {
                appendSimpleElement("symbol", value);
            }
            appendOptionFooter();
            return this;
        }

        public PropBuilder appendBoolean(String optName)
        {
            appendOptionHeader(optName, "boolean");
            appendOptionFooter();
            return this;
        }

        public PropBuilder appendBooleanTrueFalse(String optName)
        {
            appendOptionHeader(optName, "boolean_true_false");
            appendOptionFooter();
            return this;
        }

        public PropBuilder appendString(String optName)
        {
            appendOptionHeader(optName, "string");
            appendOptionFooter();
            return this;
        }

        public WhitelistProps build()
        {
            return build(LinStorObject.CTRL);
        }

        public WhitelistProps build(LinStorObject lsObj)
        {
            xmlBuilder.append(xmlFooter);
            whitelistProps.appendRules(
                true,
                new ByteArrayInputStream(xmlBuilder.toString().getBytes()),
                null,
                false,
                lsObj
            );
            return whitelistProps;
        }

        private void appendOptionHeader(String name, String type)
        {
            xmlBuilder
                .append("<option name=\"").append(name)
                .append("\" type=\"")
                .append(type)
                .append("\">\n");
        }

        private void appendOptionFooter()
        {
            xmlBuilder.append("</option>\n");
        }

        private void appendSimpleElement(String tagName, String content)
        {
            xmlBuilder
                .append("\t<")
                .append(tagName)
                .append(">")
                .append(content)
                .append("</")
                .append(tagName)
                .append(">\n");
        }
    }
}
