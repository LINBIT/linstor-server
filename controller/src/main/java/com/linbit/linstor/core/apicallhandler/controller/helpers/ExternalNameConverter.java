package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceName;

import java.util.UUID;

public class ExternalNameConverter
{
    public static final String UUID_NAME_PREFIX = "LS_";

    private static final int MAX_INPUT_LENGTH = 256;
    private static final int ALPHABET_CHARS = 26;
    private static final int CASE_ALPHABET_CHARS = 2 * ALPHABET_CHARS;
    private static final byte REPLACE_CHAR = '_';
    private static final byte[] REPLACEABLE_CHAR_LIST = " !@#$%^&*()+=[]{}:;\"'<>?,./~".getBytes();

    private enum Mode
    {
        PREFIX_UUID,
        COPY,
        TRANSLATE,
        FAIL;

        Mode advance()
        {
            Mode next;
            switch (this)
            {
                case PREFIX_UUID:
                    next = COPY;
                    break;
                case COPY:
                    next = TRANSLATE;
                    break;
                case TRANSLATE:
                    next = FAIL;
                    break;
                case FAIL:
                    next = FAIL;
                    break;
                default:
                    throw new ImplementationError(
                        "Unhandled enumeration value " + this.name()
                    );
            }
            return next;
        }
    }

    public static ResourceName createResourceName(final byte[] genInput, CoreModule.ResourceDefinitionMap rscDfnMap)
        throws InvalidNameException
    {
        ResourceName rscName = null;
        if (genInput.length > MAX_INPUT_LENGTH)
        {
            throw new InvalidNameException(
                "Resource name generation failed, data input length of " + genInput.length + " bytes exceeds " +
                "maximum length of " + MAX_INPUT_LENGTH + " bytes",
                new String(genInput)
            );
        }
        else
        if (genInput.length > 0)
        {
            String truncName = new String(genInput, 0, Math.min(genInput.length, ResourceName.MAX_LENGTH));
            for (Mode genMode = Mode.PREFIX_UUID; rscName == null && genMode != Mode.FAIL; genMode = genMode.advance())
            {
                switch (genMode)
                {
                    case PREFIX_UUID:
                        // If the (possibly truncated) input data parses successfully as a text representing a UUID,
                        // prefix it and regenerate the UUID text representation from the numeric data
                        try
                        {
                            String uuidStr = UUID_NAME_PREFIX + UUID.fromString(truncName).toString().toUpperCase();
                            rscName = new ResourceName(uuidStr);
                        }
                        catch (IllegalArgumentException ignored)
                        {
                        }
                        break;
                    case COPY:
                        // If the (possibly truncated) input data can be used as a valid resource name,
                        // use it without further modification
                        try
                        {
                            rscName = new ResourceName(truncName);
                        }
                        catch (InvalidNameException ignored)
                        {
                        }
                        break;
                    case TRANSLATE:
                        // Generate a valid resource name by translating characters that are
                        // not valid in resource names to other characters that are valid
                        rscName = translateGenInput(genInput);
                        break;
                    case FAIL:
                        // fall-through
                    default:
                        throw new ImplementationError("Unhandled enumeration value " + genMode.name());
                }
            }
        }
        if (rscName == null || rscDfnMap.containsKey(rscName))
        {
            String uuidStr = UUID_NAME_PREFIX + UUID.randomUUID().toString();
            rscName = new ResourceName(uuidStr);
        }
        return rscName;
    }

    private static @Nullable ResourceName translateGenInput(final byte[] genInput)
    {
        ResourceName rscName = null;
        int inputLength = Math.min(genInput.length, ResourceName.MAX_LENGTH);
        byte[] genOutput = new byte[inputLength];

        for (int idx = 0; idx < inputLength; ++idx)
        {
            if (isValidChar(genInput[idx], idx >= 1))
            {
                // Valid characters in the input data are copied
                genOutput[idx] = genInput[idx];
            }
            else
            if (isReplaceableChar(genInput[idx]))
            {
                // Most punctuation characters that are rather unlikely to be the only significant
                // difference between two names are all replaced by a single replacement character
                genOutput[idx] = REPLACE_CHAR;
            }
            else
            {
                // More critical invalid characters are translated to lower the likeliness of name collisions
                byte genChar = (byte) Math.abs(genInput[idx] % CASE_ALPHABET_CHARS);
                genOutput[idx] = (byte) (genChar < ALPHABET_CHARS ? 'A' + genChar : 'a' + (genChar - ALPHABET_CHARS));
            }
        }
        try
        {
            rscName = new ResourceName(new String(genOutput));
        }
        catch (InvalidNameException ignored)
        {
        }
        return rscName;
    }

    private static boolean isValidChar(final byte curChar, final boolean innerChar)
    {
        boolean validFlag;
        if (curChar >= 'a' && curChar <= 'z' ||
            curChar >= 'A' && curChar <= 'Z' ||
            innerChar && curChar >= '0' && curChar <= '9')
        {
            validFlag = true;
        }
        else
        {
            final byte[] validChars = innerChar ? ResourceName.VALID_INNER_CHARS : ResourceName.VALID_CHARS;
            int idx = 0;
            while (idx < validChars.length && validChars[idx] != curChar)
            {
                ++idx;
            }
            validFlag = idx < validChars.length;
        }
        return validFlag;
    }

    private static boolean isReplaceableChar(final byte curChar)
    {
        int idx = 0;
        while (idx < REPLACEABLE_CHAR_LIST.length && REPLACEABLE_CHAR_LIST[idx] != curChar)
        {
            ++idx;
        }
        return idx < REPLACEABLE_CHAR_LIST.length;
    }

    private ExternalNameConverter()
    {
    }
}
