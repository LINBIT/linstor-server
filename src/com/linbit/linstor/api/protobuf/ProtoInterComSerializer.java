/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgHeaderOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntPrimaryOuterClass;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.linbit.linstor.api.interfaces.serializer.InterComSerializer;

/**
 *
 * @author rpeinthor
 */
public class ProtoInterComSerializer implements InterComSerializer {
    private final ErrorReporter errReporter;

    public ProtoInterComSerializer()
    {
        this.errReporter = null;
    }

    public ProtoInterComSerializer(final ErrorReporter errReporter)
    {
        this.errReporter = errReporter;
    }

    public byte[] buildMessage(byte[] header, byte[] payload)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(header);
            baos.write(payload);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
        return baos.toByteArray();
    }

    @Override
    public byte[] getHeader(String apiCall, int msgId)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            MsgHeaderOuterClass.MsgHeader.newBuilder()
                .setApiCall(apiCall)
                .setMsgId(msgId)
                .build()
                .writeDelimitedTo(baos);

            return baos.toByteArray();
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
        return baos.toByteArray();
    }

    @Override
    public byte[] getPrimaryRequest(String rscName, String rscUuid) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            
            MsgIntPrimaryOuterClass.MsgIntPrimary.Builder msgReqPrimary = MsgIntPrimaryOuterClass.MsgIntPrimary.newBuilder();
            msgReqPrimary.setRscName(rscName);
            msgReqPrimary.setRscUuid(rscUuid);

            msgReqPrimary.build().writeDelimitedTo(baos);
        } catch (IOException ex) {
            errReporter.reportError(ex);
        }
        return baos.toByteArray();
    }
}
