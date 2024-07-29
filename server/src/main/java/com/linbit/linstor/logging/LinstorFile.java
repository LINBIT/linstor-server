package com.linbit.linstor.logging;

import com.linbit.linstor.annotation.Nullable;

import java.util.Date;
import java.util.Optional;

public class LinstorFile implements Comparable<LinstorFile>
{
    protected Date dateTime;
    protected String fileName;
    protected @Nullable String text;
    protected String nodeName;

    public LinstorFile(final String nodeNameRef, final String fileNameRef, Date dateRef)
    {
        nodeName = nodeNameRef;
        fileName = fileNameRef;
        dateTime = dateRef;
    }

    public LinstorFile(final String nodeNameRef, final String fileNameRef, Date dateRef, @Nullable final String textRef)
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

    public Optional<String> getText()
    {
        return Optional.ofNullable(text);
    }

    public void setText(@Nullable String newText)
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
