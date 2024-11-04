package com.linbit.linstor.core.identifier;

import com.linbit.InvalidNameException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import com.google.common.base.Objects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Valid name of a linstor external file.
 * This is NOT the path the external file should be deployed at, but more
 * a short descriptive name for this entry.
 */
public class ExternalFileName implements Comparable<ExternalFileName>
{
    public static final int MAX_LENGTH = 1024;
    public final String extFileName;

    public ExternalFileName(String extFileNameRef) throws InvalidNameException
    {
        if (extFileNameRef == null)
        {
            throw new InvalidNameException("Invalid Name: Name must not be null", extFileNameRef);
        }
        if (extFileNameRef.length() > MAX_LENGTH)
        {
            throw new InvalidNameException(
                String.format(
                    "Invalid name: Name length %d is greater than maximum length %d",
                    extFileNameRef.length(),
                    MAX_LENGTH
                ),
                extFileNameRef
            );
        }
        if (!Paths.get(extFileNameRef).isAbsolute())
        {
            throw new InvalidNameException(
                "Invalid name: The path must be absolute!",
                extFileNameRef
            );
        }
        if (!StandardCharsets.US_ASCII.newEncoder().canEncode(extFileNameRef))
        {
            throw new InvalidNameException(
                "Invalid name: The path must contain only ASCII characters",
                extFileNameRef
            );
        }
        extFileName = extFileNameRef;
    }

    @Override
    public int compareTo(ExternalFileName otherExtNameRef)
    {
        return extFileName.compareTo(otherExtNameRef.extFileName);
    }

    @Override
    public int hashCode()
    {
        return extFileName.hashCode();
    }

    @SuppressFBWarnings("EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS")
    @Override
    public boolean equals(Object obj)
    {
        boolean eq;
        if (this == obj)
        {
            eq = true;
        }
        else
        {
            if (obj instanceof ExternalFileName)
            {
                ExternalFileName other = (ExternalFileName) obj;
                eq = Objects.equal(extFileName, other.extFileName);
            }
            else
            if (obj instanceof String)
            {
                eq = Objects.equal(extFileName, obj);
            }
            else
            {
                eq = false;
            }
        }
        return eq;
    }
}
