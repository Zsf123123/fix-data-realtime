package com.muheda.msgEnCoder;

import com.esotericsoftware.kryo.Kryo;
import com.muheda.domain.Msg;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;


import java.io.ByteArrayOutputStream;

import org.apache.commons.io.IOUtils;


import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;


public class MsgEncoder extends MessageToByteEncoder<Msg> {


    private Kryo kryo = new Kryo();

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Msg msg, ByteBuf byteBuf) throws Exception {


        byte[] body = convertToBytes(msg);


    }


    private byte[] convertToBytes(Msg msg) {

        ByteArrayOutputStream bos = null;
        Output output = null;
        try {
            bos = new ByteArrayOutputStream();
            output = new Output(bos);
            kryo.writeObject(output, msg);
            output.flush();

            return bos.toByteArray();
        } catch (KryoException e) {
            e.printStackTrace();
        }finally{
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(bos);
        }
        return null;
    }

}
