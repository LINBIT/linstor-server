package com.linbit.linstor.logging;

import java.util.Date;

public class LinstorFile implements Comparable<LinstorFile>
{
    protected Date dateTime;
    protected String fileName;
    protected String text;
    protected String nodeName;

    public LinstorFile(final String nodeNameRef, final String fileNameRef, Date dateRef)
    {
        nodeName = nodeNameRef;
        fileName = fileNameRef;
        dateTime = dateRef;
    }

    public LinstorFile(final String nodeNameRef, final String fileNameRef, Date dateRef, final String textRef)
    {
        nodeName = nodeNameRef;
        fileName = fileNameRef;
        dateTime = dateRef;
        text = textRef;
    }

    public Date getDateTime()
    {
        return dateTime;
    }

    public String getFileName()
    {
        return fileName;
    }

    public String getText()
    {
        return text;
    }

    public void setText(String newText)
    {
        text = newText;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    @Override
    public int compareTo(LinstorFile errRpt)
    {
        int cmp = dateTime.compareTo(errRpt.dateTime);
        if (cmp == 0)
        {
            cmp = nodeName.compareTo(errRpt.nodeName);
            if (cmp == 0)
            {
                cmp = fileName.compareTo(errRpt.fileName);
            }
        }
        return cmp;
    }
}
