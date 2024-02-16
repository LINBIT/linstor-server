package com.linbit.linstor.logging;

import java.util.Arrays;

public class ErrorReportRenderer
{
    public static final int DFLT_INDENT_INCREMENT = 4;
    private static final int INDENT_CACHE_LEN = 40;
    private static final char[] INDENT_CACHE;

    static
    {
        INDENT_CACHE = new char[INDENT_CACHE_LEN];
        Arrays.fill(INDENT_CACHE, ' ');
    }

    private final StringBuilder result = new StringBuilder();

    private final int indentIncr;

    private int indentInt = 0;
    private boolean needsIndent = true;

    public ErrorReportRenderer()
    {
        this(DFLT_INDENT_INCREMENT);
    }

    public ErrorReportRenderer(int indentIncrRef)
    {
        indentIncr = indentIncrRef;
    }

    public void increaseIndent()
    {
        increaseIncrement(indentIncr);
    }

    public void increaseIncrement(int indentIncrRef)
    {
        indentInt += indentIncrRef;
    }

    public void decreaseIndent()
    {
        decreaseIndent(indentIncr);
    }

    public void decreaseIndent(int indentIncrRef)
    {
        indentInt = Math.max(0, indentInt - indentIncrRef);
    }

    public String getErrorReport()
    {
        return result.toString();
    }

    public ErrorReportRenderer println()
    {
        result.append("\n");
        needsIndent = true;
        return this;
    }

    public ErrorReportRenderer println(String format, Object... data)
    {
        printf(format, data);
        result.append("\n");
        needsIndent = true;
        return this;
    }

    public ErrorReportRenderer println(int tmpIndentRef, String format, Object... data)
    {
        increaseIncrement(tmpIndentRef);
        println(format, data);
        decreaseIndent(tmpIndentRef);
        return this;
    }

    public ErrorReportRenderer printlnWithIndent(String format, Object... data)
    {
        println(DFLT_INDENT_INCREMENT, format, data);
        return this;
    }

    public ErrorReportRenderer printf(String formatRef, Object... dataRef)
    {
        if (dataRef == null || dataRef.length == 0)
        {
            print(formatRef);
        }
        else
        {
            print(String.format(formatRef, dataRef));
        }
        return this;
    }

    public ErrorReportRenderer print(String str)
    {
        if (str != null)
        {
            if (str.isBlank())
            {
                String newLinesOnly = str.replaceAll("[^\n]", "");
                result.append(newLinesOnly);
                needsIndent = !newLinesOnly.isEmpty();
            }
            else
            {
                if (indentInt == 0)
                {
                    result.append(str);
                }
                else
                {
                    if (needsIndent)
                    {
                        addIndent();
                    }
                    char[] data = str.toCharArray();
                    int offset = 0;
                    for (int idx = 0; idx < data.length; ++idx)
                    {
                        if (data[idx] == '\n')
                        {
                            if (idx > offset)
                            {
                                addIndent();
                                result.append(data, offset, idx - offset);
                            }
                            result.append('\n');
                            offset = idx + 1;
                        }
                    }
                    if (offset < data.length)
                    {
                        addIndent();
                        result.append(data, offset, data.length - offset);
                    }
                }
                needsIndent = str.endsWith("\n");
            }
        }
        return this;
    }

    private void addIndent()
    {
        for (int indentToWrite = indentInt; indentToWrite > 0; indentToWrite -= INDENT_CACHE_LEN)
        {
            result.append(INDENT_CACHE, 0, Math.min(indentToWrite, INDENT_CACHE_LEN));
        }
    }
}
