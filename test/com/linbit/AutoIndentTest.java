package com.linbit;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.Before;
import org.junit.Test;

public class AutoIndentTest
{

    ByteArrayOutputStream baos;
    PrintStream ps;

    @Before
    public void setUp()
    {
        clearBuffer();
    }

    protected String getString()
    {
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    protected String getString(int indent, String text)
    {
        AutoIndent.printWithIndent(ps, indent, text);
        return getString();
    }

    protected String getSpaces(int count)
    {
        StringBuilder sb = new StringBuilder();
        while(count --> 0)
        {
            sb.append(" ");
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
        for (int indent = 1; indent < 10; indent++)
        {
            assertEquals("Failed with indent: "+indent,
                getSpaces(indent)+"simple\n"+getSpaces(indent)+"test\n",
                getString(indent, "simple\ntest"));
            clearBuffer();
        }
    }
}
