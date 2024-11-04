package com.linbit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AutoIndentTest
{

    ByteArrayOutputStream baos;
    PrintStream ps;

    @Before
    public void setUp()
    {
        clearBuffer();
    }

    // baos is initialized in clearBuffer, which is called in setUp, which is @Before
    @SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
    protected String getString()
    {
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    protected String getString(int indent, String text)
    {
        AutoIndent.printWithIndent(ps, indent, text);
        return getString();
    }

    protected String getSpaces(int countRef)
    {
        int count = countRef;
        StringBuilder sb = new StringBuilder();
        while (count > 0)
        {
            sb.append(" ");
            --count;
        }
        return sb.toString();
    }

    void clearBuffer()
    {
        baos = new ByteArrayOutputStream();
        ps = new PrintStream(baos);
    }

    @Test
    public void testVerySimple()
    {
        String text = "very simple test";
        assertEquals(text + "\n", getString(0, text));
    }

    @Test
    public void testVerySimpleWithNewLine()
    {
        String text = "very \nsimple test";
        assertEquals(text + "\n", getString(0, text));
    }

    @Test
    public void testVerySimpleWithNewLineAndTabs()
    {
        String text = "very \nsimple \t\ttest";
        assertEquals(text + "\n", getString(0, text));
    }

    @Test
    public void testSimpleIndent()
    {
        assertEquals(" simple\n test\n", getString(1, "simple\ntest"));
        clearBuffer();
        assertEquals(" simple\n test with\n more\n\n\n lines\n",
            getString(1, "simple\ntest with\nmore\n\n\nlines"));
    }

    @Test
    public void testSimpleIndentPreservingWhitespace()
    {
        assertEquals(" simple\n  test\n", getString(1, "simple\n test"));
        clearBuffer();
        assertEquals(" simple\n \ttest\n", getString(1, "simple\n\ttest"));
    }

    @Test
    public void testLargeIndent()
    {
        final int count = 10;
        for (int indent = 1; indent < count; indent++)
        {
            assertEquals("Failed with indent: " + indent,
                getSpaces(indent) + "simple\n" + getSpaces(indent) + "test\n",
                getString(indent, "simple\ntest"));
            clearBuffer();
        }
    }
}
