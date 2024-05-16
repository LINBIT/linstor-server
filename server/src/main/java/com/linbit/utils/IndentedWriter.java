package com.linbit.utils;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

public class IndentedWriter
{
    private static final String DFLT_MULTILINE_SUFFIX = ":";
    private static final String DFLT_SINGLELINE_SUFFIX = ": ";
    private static final String DFLT_NEW_LINE_INDENT = "   ";
    private static final String DFLT_GLOBAL_INDENT = "";

    private final StringBuilder out;
    private String indentGlobal = DFLT_GLOBAL_INDENT;
    private String indentNewLine = DFLT_NEW_LINE_INDENT;

    private String multilineSuffix = DFLT_MULTILINE_SUFFIX;
    private String singlelineSuffix = DFLT_SINGLELINE_SUFFIX;

    public IndentedWriter()
    {
        this(new StringBuilder());
    }

    @Override
    public String toString()
    {
        return out.toString();
    }

    public IndentedWriter(StringBuilder stringBuilderRef)
    {
        out = stringBuilderRef;
    }

    public IndentedWriter withIndentGlobal(String indentGlobalRef)
    {
        indentGlobal = indentGlobalRef;
        return this;
    }

    public IndentedWriter withIndentNewLine(String indentNewLineRef)
    {
        indentNewLine = indentNewLineRef;
        return this;
    }

    public IndentedWriter withMultilineSuffixSingular(String multilineSuffixRef)
    {
        multilineSuffix = multilineSuffixRef;
        return this;
    }

    public IndentedWriter withSinglelineSuffix(String singlelineSuffixRef)
    {
        singlelineSuffix = singlelineSuffixRef;
        return this;
    }

    public <T> IndentedWriter appendIfExists(String descriptionRef, @Nullable T elem)
    {
        appendIfExists(descriptionRef + singlelineSuffix, elem, "\n");
        return this;
    }

    public <T> IndentedWriter appendIfExists(
        @Nullable String prefixRef,
        @Nullable T elemRef,
        @Nullable String suffixRef
    )
    {
        if (elemRef != null)
        {
            out.append(indentGlobal);
            if (prefixRef != null)
            {
                out.append(prefixRef);
            }
            out.append(elemRef);
            if (suffixRef != null)
            {
                out.append(suffixRef);
            }
        }
        return this;
    }

    public <T> IndentedWriter appendIfNotEmptyNoPlural(String descriptionRef, @Nullable Collection<T> collectionRef)
    {
        return appendIfNotEmpty(
            descriptionRef + multilineSuffix,
            descriptionRef + multilineSuffix,
            indentNewLine,
            collectionRef
        );
    }

    public <T> IndentedWriter appendIfNotEmpty(String descriptionRef, @Nullable Collection<T> collectionRef)
    {
        return appendIfNotEmpty(
            descriptionRef + multilineSuffix,
            descriptionRef + "s" + multilineSuffix,
            indentNewLine,
            collectionRef
        );
    }

    public <T> IndentedWriter appendIfNotEmpty(
        String descriptionSingularRef,
        String descriptionPluralRef,
        @Nullable Collection<T> collectionRef
    )
    {
        return appendIfNotEmpty(
            descriptionSingularRef + multilineSuffix,
            descriptionPluralRef + multilineSuffix,
            indentNewLine,
            collectionRef
        );
    }

    public <T> IndentedWriter appendIfNotEmpty(
        @Nullable String descriptionSingularRef,
        @Nullable String descriptionPluralRef,
        String indentRef,
        @Nullable Collection<T> collectionRef
    )
    {
        if (collectionRef != null && !collectionRef.isEmpty())
        {
            out.append(indentGlobal);
            if (collectionRef.size() == 1)
            {
                if (descriptionSingularRef != null)
                {
                    out.append(descriptionSingularRef).append("\n");
                }
            }
            else
            {
                if (descriptionPluralRef != null)
                {
                    out.append(descriptionPluralRef).append("\n");
                }
            }
            for (T elem : collectionRef)
            {

                out.append(indentGlobal)
                    .append(indentRef)
                    .append(elem)
                    .append("\n");
            }
        }
        return this;
    }

    public <K, V> IndentedWriter appendIfNotEmptyNoPlural(@Nullable String descriptionRef, @Nullable Map<K, V> mapRef)
    {
        return appendIfNotEmpty(
            descriptionRef + multilineSuffix,
            descriptionRef + multilineSuffix,
            indentNewLine,
            mapRef
        );
    }

    public <K, V> IndentedWriter appendIfNotEmpty(@Nullable String descriptionRef, @Nullable Map<K, V> mapRef)
    {
        return appendIfNotEmpty(
            descriptionRef + multilineSuffix,
            descriptionRef + "s" + multilineSuffix,
            indentNewLine,
            mapRef
        );
    }

    public <K, V> IndentedWriter appendIfNotEmpty(
        String descriptionSingularRef,
        String descriptionPluralRef,
        @Nullable Map<K, V> mapRef
        )
    {
        return appendIfNotEmpty(
            descriptionSingularRef + multilineSuffix,
            descriptionPluralRef + multilineSuffix,
            indentNewLine,
            mapRef
        );
    }

    public <K, V> IndentedWriter appendIfNotEmpty(
        @Nullable String descriptionSingularRef,
        @Nullable String descriptionPluralRef,
        String indentRef,
        @Nullable Map<K, V> mapRef
    )
    {
        if (mapRef != null && !mapRef.isEmpty())
        {
            out.append(indentGlobal);
            if (mapRef.size() == 1)
            {
                if (descriptionSingularRef != null)
                {
                    out.append(descriptionSingularRef).append("\n");
                }
            }
            else
            {
                if (descriptionPluralRef != null)
                {
                    out.append(descriptionPluralRef).append("\n");
                }
            }
            for (Map.Entry<K, V> elem : mapRef.entrySet())
            {
                out.append(indentGlobal)
                    .append(indentRef)
                    .append(elem.getKey())
                    .append(": ")
                    .append(elem.getValue())
                    .append("\n");
            }
        }
        return this;
    }
}
