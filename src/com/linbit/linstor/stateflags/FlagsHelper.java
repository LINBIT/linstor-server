package com.linbit.linstor.stateflags;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * FlagsHelper contains static helper methods to avoid code duplication in flag enums.
 * @author rp
 */
public class FlagsHelper
{

    public static <E extends Enum<E> & Flags> List<String> 
        toStringList(Class<E> enumClass, long rscFlags)
     {
         EnumSet<E> values = EnumSet.allOf(enumClass);
         List<String> strList = new ArrayList<>(values.size());
         for (E en : values)
         {
             if ((rscFlags & en.getFlagValue()) == en.getFlagValue())
             {
                 strList.add(en.toString());
             }
         }
         return strList;
     }

     public static <E extends Enum<E> & Flags> long 
        fromStringList(Class<E> enumClass, List<String> listFlags)
     {
         long value = 0;

         for (String sFlag : listFlags)
         {
             value |= E.valueOf(enumClass, sFlag).getFlagValue();
         }

         return value;
     }
}
