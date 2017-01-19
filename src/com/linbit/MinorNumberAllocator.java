package com.linbit;

import java.util.Arrays;

/**
 *
 * @author rblauen
 */
public class MinorNumberAllocator
{
    /**
     * Retrieves a free (unused) minor number
     * Minor numbers are allocated in the range from the configuration value
     * KEY_MIN_MINOR_NR to the constant MinorNr.MINOR_NR_MAX. <br>
     * A minor number that is unique across the drbdmanage cluster is allocated for each
     * volume.
     *
     * @param occupied_list int[] filled with the minor numbers
     * @return int - next free minor number; or -1 on error
     */
    public int get_free_minor_nr(int[] occupied_list)
    {
        //try
        //{
        int min_nr = MinorNr.KEY_MIN_MINOR_NR;
        int minor_nr = 0;   // ?

        int occupied_list_length = occupied_list.length;
        if (occupied_list_length > 0)
        {
            Arrays.sort(occupied_list);
            int chk_idx = bisect_left(occupied_list, minor_nr);
            if (chk_idx != occupied_list_length && occupied_list[chk_idx] == minor_nr)
            {
                /* Number is in use, recycle a free minor number
                 *
                 * Try finding a free number in the range of numbers
                 * greater than the current minor number */
                int free_nr = get_free_number(minor_nr, MinorNr.MINOR_NR_MAX, occupied_list, true);
                if (free_nr == -1)
                {
                    /* No free numbers in the high range, try finding a free number
                     * in the range of numbers less than the current minor number */
                    free_nr = get_free_number(min_nr, minor_nr, occupied_list, true);
                    if (free_nr == -1)
                    {
                        /* All minor numbers are occupied */
                        // throw some kind of exception
                    }
                }
                minor_nr = free_nr;
            }
        }
        int next_number = minor_nr + 1;
        if (next_number > MinorNr.MINOR_NR_MAX)
        {
            next_number = min_nr;
        }
        // cluster_conf.set_prop(KEY_CUR_MINOR_NR, str(next_number))
        //}
        //catch (Exception ex)
        //{
        //minor_nr = MinorNr.MINOR_NR_ERROR;
        //}
        return minor_nr;
    }

    /**
     * Returns the first number in the range min_nr..max_nr that is not in nr_list. <br>
     *
     * In the range min_nr to max_nr, finds and returns a number that is not in the
     * supplied list of numbers.<br>
     * min_nr and max_nr must be >= 0, and nr_list must be a list of integer numbers
     *
     * @param min_nr min-value: range start, >=0
     * @param max_nr max-value: range end, >= 0, greater than or equl to min_nr
     * @param nr_list int[] filled with the minor numbers
     * @param nr_sorted true if the nr_list should be sorted
     * @return int - first free number within min_nr and max_nr; or -1 on error
     */
    public int get_free_number(int min_nr, int max_nr, int[] nr_list, boolean nr_sorted)
    {
        int free_nr = -1;

        if (!nr_sorted)
        {
            Arrays.sort(nr_list);
        }

        if (min_nr >= 0 && min_nr <= max_nr)
        {
            int nr_list_length = nr_list.length;
            int index = 0;
            int number = min_nr;
            while (number <= max_nr && free_nr == -1)
            {
                boolean occupied = false;
                while (index < nr_list_length)
                {
                    if (nr_list[index] >= number)
                    {
                        if (nr_list[index] == number)
                        {
                            occupied = true;
                        }
                        break;
                    }
                    index += 1;
                }
                if (occupied)
                {
                    index += 1;
                }
                else
                {
                    free_nr = number;
                }
                number += 1;
            }
        }

        return free_nr;
    }

    /**
     * Java-implementation of python's bisect_left()
     *
     * @param numbers int[] filled with the minor numbers
     * @param nr int the number that shall be inserted
     * @return int the insertion point for nr
     */
    public int bisect_left(int[]numbers, int nr)
    {
        int x = Arrays.binarySearch(numbers, nr);
        if (x < 0)
        {
            x = (x * (-1)) -1;
        }
        return x;
    }

    /* For debugging purposes! */
    private static class MinorNr
    {
        // =============================================================================================
        // Attributes
        // =============================================================================================
        private static int MINOR_NR_MAX = (1 << 20 ) -1;
        private static int KEY_MIN_MINOR_NR = 0;
    }

    /* MAIN */
    public static void main(String[] args)
    {
        MinorNumberAllocator mna = new MinorNumberAllocator();
        int[]numbers = new int[]{0,1,2,3,4,6,7,8,10};
        int[]nrs = new int[]{-1, 100, 5, 6, 9};

//    int free_number = MinorNumberAllocator.get_free_number(0, 20, numbers, false);
//    System.out.println(free_number);

        int in0 = Arrays.binarySearch(numbers, nrs[0]);
        int in1 = Arrays.binarySearch(numbers, nrs[1]);
        int in2 = Arrays.binarySearch(numbers, nrs[2]);
        int in3 = Arrays.binarySearch(numbers, nrs[3]);
        int in4 = Arrays.binarySearch(numbers, nrs[4]);

        // original output
        System.out.println(nrs[0] + ": " + in0);
        System.out.println(nrs[1] + ": " + in1);
        System.out.println(nrs[2] + ": " + in2);
        System.out.println(nrs[3] + ": " + in3);
        System.out.println(nrs[4] + ": " + in4);

        System.out.println("----------------------------------------------");

        // modified output
        System.out.println(nrs[0] + ": " + mna.bisect_left(numbers, nrs[0]));
        System.out.println(nrs[1] + ": " + mna.bisect_left(numbers, nrs[1]));
        System.out.println(nrs[2] + ": " + mna.bisect_left(numbers, nrs[2]));
        System.out.println(nrs[3] + ": " + mna.bisect_left(numbers, nrs[3]));
        System.out.println(nrs[4] + ": " + mna.bisect_left(numbers, nrs[4]));
    }
}
