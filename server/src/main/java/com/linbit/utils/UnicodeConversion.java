// From https://github.com/raltnoeder/UtfRecoder
// Modified for inclusion in LINSTOR
package com.linbit.utils;

/**
 * Copyright (C) 2021 Robert ALTNOEDER
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are permitted provided that
 * the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the distribution.
 *  3. The name of the author may not be used to endorse or promote products
 *     derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @author Robert Altnoeder &lt;r.altnoeder@gmx.net&gt;
 */
import com.linbit.ImplementationError;
import java.util.Arrays;

public class UnicodeConversion
{
    enum ConversionState
    {
        FIRST_UNIT,
        CONT_UNIT
    };

    public static char[] utf8BytesToUtf16Chars(byte[] input, final boolean secure)
        throws InvalidSequenceException
    {
        // All UTF-8 sequences up to 3 bytes can be encoded in a single (16-bit) UTF-16 character
        // Therefore, all 1 byte, 2 byte and 3 byte sequences will use 1 char, and only 4 byte sequences
        // will use 2 chars. As a conclusion, it is sufficient to allocate as many characters for the
        // output text as there are bytes in the input text.
        char[] converted = new char[input.length];

        final int convIdx;
        try
        {
            convIdx = utf8BytesToUtf16CharsImpl(input, converted);
        }
        catch (Exception exc)
        {
            if (secure)
            {
                // Wipe the buffer before rethrowing the exception
                Arrays.fill(converted, (char) 0);
            }
            throw exc;
        }

        char[] output;
        if (convIdx < converted.length)
        {
            // Adjust the length of the output array and copy the converted text, then wipe the internal buffer
            output = new char[convIdx];
            for (int idx = 0; idx < convIdx; ++idx)
            {
                output[idx] = converted[idx];
            }
            if (secure)
            {
                Arrays.fill(converted, (char) 0);
            }
        }
        else
        {
            // Use the internal buffer as output
            output = converted;
        }

        return output;
    }

    private static int utf8BytesToUtf16CharsImpl(byte[] input, char[] converted)
        throws InvalidSequenceException
    {
        int convIdx = 0;

        ConversionState state = ConversionState.FIRST_UNIT;
        int inIdx = 0;
        int contLength = 1;
        int contIndex = 0;
        int codePoint = 0;
        while (inIdx < input.length)
        {
            byte inByte = input[inIdx];
            switch (state)
            {
                case FIRST_UNIT:
                {
                    if ((inByte & 0x80) != 0)
                    {
                        if ((inByte & 0xE0) == 0xC0)
                        {
                            // Begin of a 2-byte UTF-8 sequence
                            contLength = 1;
                            codePoint = inByte & 0x1F;
                        }
                        else
                        if ((inByte & 0xF0) == 0xE0)
                        {
                            // Begin of a 3-byte UTF-8 sequence
                            contLength = 2;
                            codePoint = inByte & 0x0F;
                        }
                        else
                        if ((inByte & 0xF8) == 0xF0)
                        {
                            // Begin of a 4-byte UTF-8 sequence
                            contLength = 3;
                            codePoint = inByte & 0x07;
                        }
                        else
                        {
                            // Illegal extended ASCII byte that should have been encoded
                            // as a 2-byte UTF-8 sequence
                            throw new InvalidSequenceException();
                        }
                        // The next byte is a continuation byte
                        contIndex = 0;
                        state = ConversionState.CONT_UNIT;
                    }
                    else
                    {
                        // Single ASCII byte
                        converted[convIdx] = (char) inByte;
                        ++convIdx;
                    }
                    break;
                }
                case CONT_UNIT:
                {
                    if ((inByte & 0xC0) != 0x80)
                    {
                        // Not a UTF-8 continuation byte
                        throw new InvalidSequenceException();
                    }

                    codePoint <<= 6;
                    codePoint |= (inByte & 0x3F);

                    ++contIndex;
                    if (contIndex >= contLength)
                    {
                        // Sequence complete
                        if ((contLength == 1 && codePoint < 0x80) ||
                            (contLength == 2 && codePoint < 0x800) ||
                            (contLength == 3 && codePoint < 0x10000) ||
                            (codePoint >= 0xD800 && codePoint <= 0xDFFF) ||
                            codePoint > 0x10FFFF)
                        {
                            // Overlength sequence or Unicode code point out of range
                            throw new InvalidSequenceException();
                        }
                        else
                        if (contLength < 1 || contLength >= 4)
                        {
                            throw new ImplementationError(
                                UnicodeConversion.class.getSimpleName() +
                                ": Logic error: Continuation length out of range"
                            );
                        }

                        if (codePoint <= 0xFFFF)
                        {
                            converted[convIdx] = (char) codePoint;
                            ++convIdx;
                        }
                        else
                        {
                            codePoint -= 0x10000;
                            converted[convIdx] = (char) (0xD800 | ((codePoint >>> 10) & 0x3FF));
                            ++convIdx;
                            converted[convIdx] = (char) (0xDC00 | (codePoint & 0x3FF));
                            ++convIdx;
                        }

                        state = ConversionState.FIRST_UNIT;
                    }
                    break;
                }
                default:
                {
                    throw new ImplementationError(
                        UnicodeConversion.class.getSimpleName() +
                        ": Logic error: The state engine entered an illegal state "
                    );
                }
            }

            ++inIdx;
        }
        if (state != ConversionState.FIRST_UNIT)
        {
            // Incomplete sequence
            throw new InvalidSequenceException();
        }

        return convIdx;
    }

    public static byte[] utf16CharsToUtf8Bytes(char[] input, final boolean secure)
        throws InvalidSequenceException
    {
        if (input.length > Integer.MAX_VALUE / 3)
        {
            throw new IllegalArgumentException();
        }

        byte[] converted = new byte[input.length * 3];
        final int convIdx;
        try
        {
            convIdx = utf16CharsToUtf8BytesImpl(input, converted);
        }
        catch (Exception exc)
        {
            if (secure)
            {
                // Wipe the buffer before rethrowing the exception
                Arrays.fill(converted, (byte) 0);
            }
            throw exc;
        }

        byte[] output;
        if (convIdx < converted.length)
        {
            output = new byte[convIdx];
            for (int idx = 0; idx < convIdx; ++idx)
            {
                output[idx] = converted[idx];
            }
            if (secure)
            {
                Arrays.fill(converted, (byte) 0);
            }
        }
        else
        {
            output = converted;
        }

        return output;
    }

    private static int utf16CharsToUtf8BytesImpl(char[] input, byte[] converted)
        throws InvalidSequenceException
    {
        int convIdx = 0;

        ConversionState state = ConversionState.FIRST_UNIT;
        int inIdx = 0;
        int codePoint = 0;
        while (inIdx < input.length)
        {
            char curChar = input[inIdx];
            switch (state)
            {
                case FIRST_UNIT:
                {
                    final char ctrlSeq = (char) (curChar & 0xFA00);
                    if (ctrlSeq == 0xD800)
                    {
                        // High surrogate
                        codePoint = (curChar & 0x3FF) << 10;
                        state = ConversionState.CONT_UNIT;
                    }
                    else
                    if (ctrlSeq == 0xDC00)
                    {
                        // Unexpected low surrogate
                        throw new InvalidSequenceException();
                    }
                    else
                    if (curChar >= 0x800)
                    {
                        converted[convIdx] = (byte) (0xE0 | ((curChar >>> 12) & 0xF));
                        ++convIdx;
                        converted[convIdx] = (byte) (0x80 | ((curChar >>> 6) & 0x3F));
                        ++convIdx;
                        converted[convIdx] = (byte) (0x80 | (curChar & 0x3F));
                        ++convIdx;
                    }
                    else
                    if (curChar >= 0x80)
                    {
                        converted[convIdx] = (byte) (0xC0 | ((curChar >>> 6) & 0x1F));
                        ++convIdx;
                        converted[convIdx] = (byte) (0x80 | (curChar & 0x3F));
                        ++convIdx;
                    }
                    else
                    {
                        // Single ASCII character
                        converted[convIdx] = (byte) curChar;
                        ++convIdx;
                    }
                    break;
                }
                case CONT_UNIT:
                {
                    final char ctrlSeq = (char) (curChar & 0xFA00);
                    if (ctrlSeq == 0xDC00)
                    {
                        // Low surrogate
                        codePoint |= curChar & 0x3FF;
                        converted[convIdx] = (byte) (0xF0 | ((curChar >>> 18) & 0x7));
                        ++convIdx;
                        converted[convIdx] = (byte) (0x80 | ((curChar >>> 12) & 0x3F));
                        ++convIdx;
                        converted[convIdx] = (byte) (0x80 | ((curChar >>> 6) & 0x30F));
                        ++convIdx;
                        converted[convIdx] = (byte) (0x80 | (curChar & 0x3F));
                        ++convIdx;
                    }
                    else
                    {
                        throw new InvalidSequenceException();
                    }
                    state = ConversionState.FIRST_UNIT;
                    break;
                }
                default:
                {
                    throw new ImplementationError(
                        UnicodeConversion.class.getSimpleName() +
                        ": Logic error: The state engine entered an illegal state "
                    );
                }
            }
            ++inIdx;
        }
        if (state != ConversionState.FIRST_UNIT)
        {
            // Incomplete sequence
            throw new InvalidSequenceException();
        }

        return convIdx;
    }

    public static class InvalidSequenceException extends Exception
    {
    }
}
