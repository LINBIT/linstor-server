package com.linbit.linstor.debug;

import java.util.LinkedList;
import java.util.List;

public class InvalidDetailsException extends Exception
{
    private final List<String> invalidDetails = new LinkedList<>();

    InvalidDetailsException()
    {
    }

    void addInvalid(String value)
    {
        invalidDetails.add(value);
    }

    String list()
    {
        StringBuilder invList = new StringBuilder();
        for (String detail : invalidDetails)
        {
            if (invList.length() > 0)
            {
                invList.append('\n');
            }
            invList.append(detail);
        }
        return invList.toString();
    }
}
