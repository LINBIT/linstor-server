package com.linbit.linstor.modularcrypto;

import com.linbit.crypto.LengthPadding;
import com.linbit.linstor.LinStorException;
import java.security.SecureRandom;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Conceals the exact length of a plaintext message
 * See the description of the LengthPadding interface for information about the purpose of this class
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class LengthPaddingImpl implements LengthPadding
{
    public static final int MAX_MSG_LENGTH = 0xFFFF;
    public static final int BLOCK_LENGTH = 32;

    public static final int SH_BYTE = 8;
    public static final int BYTE_MASK = 0xFF;

    private SecureRandom rnd;

    @Inject
    LengthPaddingImpl(final SecureRandom rndRef)
    {
        rnd = rndRef;
    }

    @Override
    public byte[] conceal(byte[] data)
        throws LinStorException
    {
        if (data.length > MAX_MSG_LENGTH)
        {
            throw new LinStorException(
                "Input data is too long, the maximum length is " + MAX_MSG_LENGTH + " bytes",
                "Input data is too long",
                "The algorithm is limited to encoding messages with a maximum length of " + MAX_MSG_LENGTH +
                " bytes",
                "Ensure that the length of the input data is within the supported range",
                "The length of the input message was " + data.length + " bytes"
            );
        }

        byte[] header = new byte[2];
        header[0] = (byte) ((data.length >>> SH_BYTE) & BYTE_MASK);
        header[1] = (byte) (data.length & BYTE_MASK);

        // A zero-length message still creates data with a length of one block
        int encodedSize = Math.max(align(header.length + data.length, BLOCK_LENGTH), BLOCK_LENGTH);
        byte[] encodedData = new byte[encodedSize];
        System.arraycopy(header, 0, encodedData, 0, header.length);
        System.arraycopy(data, 0, encodedData, header.length, data.length);
        if (encodedSize != header.length + data.length)
        {
            byte[] padding = new byte[encodedSize - header.length - data.length];
            rnd.nextBytes(padding);
            System.arraycopy(padding, 0, encodedData, header.length + data.length, padding.length);
        }
        return encodedData;
    }

    @Override
    public byte[] retrieve(byte[] data)
        throws LinStorException
    {
        byte[] decodedData;
        try
        {
            int decodedSize = Byte.toUnsignedInt(data[0]) << SH_BYTE;
            decodedSize |= Byte.toUnsignedInt(data[1]);

            if (decodedSize > data.length - 2)
            {
                // Implausible size, there is not enough data in the encoded message
                throw new LinStorException(
                    "Incorrect size information for decoding or input data is incomplete",
                    "Invalid input data for decoding",
                    "The size information for decoding is incorrect or the input data is incomplete",
                    null,
                    null
                );
            }
            decodedData = new byte[decodedSize];
            System.arraycopy(data, 2, decodedData, 0, decodedSize);
        }
        catch (IndexOutOfBoundsException boundsExc)
        {
            throw new LinStorException(
                "Incomplete input data for decoding",
                "Invalid input data for decoding",
                "The input data is incomplete",
                null,
                null
            );
        }
        return decodedData;
    }

    private static int align(final int value, final int align)
    {
        int result = value;
        if (result % align != 0)
        {
            result = (result / align + 1) * align;
        }
        return result;
    }
}
