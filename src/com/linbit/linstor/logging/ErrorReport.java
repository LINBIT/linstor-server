package com.linbit.linstor.logging;

import java.util.Date;

public class ErrorReport implements Comparable
{
    private Date dateTime;
    private String fileName;
    private String text;
    private String nodeName;

    public ErrorReport(final String nodeNameRef, final String fileNameRef, Date dateRef)
    {
        this.nodeName = nodeNameRef;
        this.fileName = fileNameRef;
        this.dateTime = dateRef;
    }

    public ErrorReport(final String nodeNameRef, final String fileNameRef, Date dateRef, final String textRef)
    {
        this.nodeName = nodeNameRef;
        this.fileName = fileNameRef;
        this.dateTime = dateRef;
        this.text = textRef;
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

    public void setText(String text)
    {
        this.text = text;
    }

    public String getNodeName() {
        return nodeName;
    }

    @Override
    public int compareTo(Object o) {
        ErrorReport b = (ErrorReport)o;
        int cmp = this.dateTime.compareTo(b.dateTime);
        if (cmp == 0)
        {
            cmp = (this.nodeName + this.fileName).compareTo(b.nodeName + b.fileName);
        }
        return cmp;
    }
}
