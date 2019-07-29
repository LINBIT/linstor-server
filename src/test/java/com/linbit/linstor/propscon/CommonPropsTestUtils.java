package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommonPropsTestUtils
{
    public static final String FIRST_KEY = "first";
    public static final String SECOND_KEY = "second";
    public static final int FIRST_AMOUNT = 3;
    public static final int SECOND_AMOUNT = 3;

    public static void checkIfEntrySetIsValid(
        final String firstPrefix,
        final String secondPrefix,
        final int firstAmount,
        final int secondAmount,
        final Set<Entry<String, String>> entrySet
    )
    {
        checkIfEntrySetIsValid(
            firstPrefix, secondPrefix, firstAmount, secondAmount,
            entrySet, new HashMap<String, String>()
        );
    }

    public static void checkIfEntrySetIsValid(
        final String firstPrefix,
        final String secondPrefix,
        final int firstAmount,
        final int secondAmount,
        final Set<Entry<String, String>> entrySet,
        final Map<String, String> additionallyAccepted
    )
    {
        assertEquals(firstAmount * (secondAmount + 1) + 1, entrySet.size());

        final Pattern keyPattern = Pattern.compile(
            "(?:" + firstPrefix + "(\\d+))?/?(?:" + secondPrefix + "(\\d+))?"
        );

        for (final Entry<String, String> entry : entrySet)
        {
            final String key = entry.getKey();
            final String value = entry.getValue();
            Matcher matcher = keyPattern.matcher(key);
            if (matcher.find())
            {
                final String firstNr = matcher.group(1);
                final String secondNr = matcher.group(2);

                if (firstNr == null)
                {
                    assertEquals("/ was added an empty string", "", value);
                }
                else
                {
                    if (secondNr == null)
                    {
                        assertEquals(firstNr, value);
                    }
                    else
                    {
                        assertEquals(glueWithDelimiter("_", firstNr, secondNr), value);
                    }
                }
            }
            else
            {
                final String acceptedValue = additionallyAccepted.get(key);
                if (null == acceptedValue)
                {
                    fail("Unrecognized key");
                }
                else
                {
                    assertEquals(acceptedValue, value);
                }
            }
        }
    }


    public static String glue(final String... strings)
    {
        return glueWithDelimiter("/", strings);
    }

    public static String glueWithDelimiter(final String delimiter, final String... strings)
    {
        StringBuilder sb = new StringBuilder();
        for (String string : strings)
        {
            sb.append(string).append(delimiter);
        }
        sb.setLength(sb.length() - delimiter.length());

        return sb.toString();
    }


    public static void fillProps(
        final Props root,
        final String firstPrefix,
        final int firstAmount,
        final String secondPrefix,
        final int secondAmount
    )
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException
    {
        root.setProp("/", "");
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            root.setProp(firstPrefix + firstNr, firstNr);
            for (int secondIdx = 0; secondIdx < secondAmount; secondIdx++)
            {
                final String secondNr = Integer.toString(secondIdx);
                root.setProp(
                    glue(firstPrefix + firstNr, secondPrefix + secondNr),
                    glueWithDelimiter("_", firstNr, secondNr)
                );
            }
        }
    }

    public static ArrayList<Entry<String, String>> generateEntries(
        final String firstPrefix,
        final int firstAmount,
        final String secondPrefix,
        final int secondAmount
    )
    {
        final ArrayList<Entry<String, String>> entries = new ArrayList<>();
        entries.add(createEntry("", "")); // root.setProp("/", "");
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            entries.add(createEntry(firstPrefix + firstNr, firstNr));
        }
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            for (int secondIdx = 0; secondIdx < secondAmount; secondIdx++)
            {
                final String secondNr = Integer.toString(secondIdx);
                entries.add(
                    createEntry(
                        glue(firstPrefix + firstNr, secondPrefix + secondNr),
                        glueWithDelimiter("_", firstNr, secondNr)
                    )
                );
            }
        }
        return entries;
    }

    public static Entry<String, String> createEntry(String key, String value)
    {
        HashMap<String, String> map = new HashMap<>();
        map.put(key, value);
        return map.entrySet().iterator().next();
    }

    public static ArrayList<String> generateKeys(
        String firstPrefix, int firstAmount,
        String secondPrefix, int secondAmount
    )
    {
        ArrayList<String> keys = new ArrayList<>();
        keys.add(""); // root.setProp("/", "");
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            keys.add(firstPrefix + firstIdx);
        }
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            for (int secondIdx = 0; secondIdx < secondAmount; secondIdx++)
            {
                final String secondNr = Integer.toString(secondIdx);
                keys.add(glue(firstPrefix + firstNr, secondPrefix + secondNr));
            }
        }
        return keys;
    }

    public static ArrayList<String> generateValues(
        String firstPrefix, int firstAmount,
        String secondPrefix, int secondAmount
    )
    {
        ArrayList<String> values = new ArrayList<>();
        values.add(""); // root.setProp("/", "");
        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            values.add(Integer.toString(firstIdx));
        }

        for (int firstIdx = 0; firstIdx < firstAmount; firstIdx++)
        {
            final String firstNr = Integer.toString(firstIdx);
            for (int secondIdx = 0; secondIdx < secondAmount; secondIdx++)
            {
                final String secondNr = Integer.toString(secondIdx);
                values.add(glueWithDelimiter("_", firstNr, secondNr));
            }
        }
        return values;
    }

    public static void assertNextEntryEqual(
        final Iterator<Entry<String, String>> iterator,
        final String expectedKey,
        final String expectedValue
    )
    {
        assertTrue(iterator.hasNext());

        final Entry<String, String> entry = iterator.next();
        final String actualKey = entry.getKey();
        final String actualValue = entry.getValue();

        assertEquals(expectedKey, actualKey);
        assertEquals(expectedValue, actualValue);
    }

    public static <T> void assertIteratorEqual(
        final Iterator<T> iterator,
        final List<T> list,
        final boolean iteratorEnds
    )
    {
        for (final T listElement : list)
        {
            assertEquals(listElement, iterator.next());
        }

        if (iteratorEnds)
        {
            assertFalse(iterator.hasNext());
        }
    }

    private CommonPropsTestUtils()
    {
    }
}
