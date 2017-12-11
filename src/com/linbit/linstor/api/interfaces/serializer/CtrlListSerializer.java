/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.interfaces.serializer;

import java.io.IOException;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
public interface CtrlListSerializer<T> {

    byte[] getListMessage(int msgId, List<T> elements) throws IOException;
}
