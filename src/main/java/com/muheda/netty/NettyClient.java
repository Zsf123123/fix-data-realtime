package com.muheda.netty;

import com.muheda.domain.Msg;
import com.muheda.msgEnCoder.MsgDecoder;
import com.muheda.msgEnCoder.MsgEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.NettyRuntime;
import netty.EchoClientHandler;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;


public class NettyClient extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private final String host;
    private final int port;

    private  ChannelFuture channelFuture;

    private  Channel channel;

    public NettyClient() {
        this(0);
    }

    public NettyClient(int port) {
        this("localhost", port);
    }

    public NettyClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();

            b.group(group) // 注册线程池
                    .channel(NioSocketChannel.class) // 使用NioSocketChannel来作为连接用的channel类
                    .remoteAddress(new InetSocketAddress(this.host, this.port)) // 绑定连接端口和host信息
                    .handler(new ChannelInitializer<SocketChannel>() { // 绑定连接初始化器
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            System.out.println("connected...");

                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new EchoClientHandler());
                            pipeline.addLast("encoder", new StringEncoder());
                            pipeline.addLast("decode", new StringDecoder());
                            pipeline.addLast("json",new JsonObjectDecoder());

                        }

                    });


            System.out.println("created..");


            ChannelFuture cf = b.connect().sync(); // 异步连接服务器
            channelFuture = cf;
            channel = cf.channel();


            for (int i = 0; i <  100; i++) {

                sendDataToServer("{\n" +
                        "\t\"name\":\"zsf\",\n" +
                        "\t\"age\":\"10\"\n" +
                        "}");

            }


        }catch(Exception e){

            logger.error("客户端连接失败");
            e.printStackTrace();

        }



    }



    public void sendDataToServer(String msg){

        if(channel.isOpen() && channel.isWritable()){

           channel.writeAndFlush(msg);
        }


    }



    public static void main(String[] args) throws Exception {


        new NettyClient("127.0.0.1", 9999).start(); // 连接127.0.0.1/65535，并启动


    }





}
