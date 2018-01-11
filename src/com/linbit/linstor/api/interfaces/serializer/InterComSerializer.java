package com.linbit.linstor.api.interfaces.serializer;

/**
 *
 * @author rpeinthor
 */
public interface InterComSerializer {
    public byte[] buildMessage(byte[] header, byte[] payload);

    public byte[] getHeader(String apiCall, int msgId);

    public byte[] getPrimaryRequest(String rscName, String rscUuid);
}
