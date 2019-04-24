package com.muheda.thrift.client;

import com.muheda.domain.ThriftSend;
import com.muheda.utils.ReadProperty;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com .muheda.thrift.produce.LngAndLat;
import com.muheda.thrift.produce.DrivingRouteData;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ThriftClient {


    private static Logger logger = Logger.getLogger(ThriftClient.class);

    private static Map<Long, ThriftSend> clients = new ConcurrentHashMap<>();
    private static String SERVER_IP = ReadProperty.getConfigData("thrift.server.ip");
    private static Integer SERVER_PORT = Integer.valueOf(ReadProperty.getConfigData("thrift.server.port"));
    private static Integer TIMEOUT = Integer.valueOf(ReadProperty.getConfigData("thrift.server.timeout"));




    private static ThriftSend getThriftSend() {

        TFramedTransport transport = new TFramedTransport(new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT));

        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        DrivingRouteData.Client client = new DrivingRouteData.Client(protocol);

        try {
            transport.open();
        } catch (TTransportException e) {
            e.printStackTrace();
        }

        ThriftSend thriftSend = new ThriftSend();
        thriftSend.setClient(client);
        thriftSend.setTransport(transport);
        return thriftSend;



    }

    /**
     * @return
     * @desc 获取当前线程对应的client
     */
    public   ThriftSend gtCurrentThread() {

        long id = Thread.currentThread().getId();
        ThriftSend thriftSend = clients.get(id);
        return thriftSend;
    }




    /**
     *
     * @desc 通过rpc 传输将设备号传到服务端
     */
    public  boolean sendDrivingRoute(String deviceId, List<LngAndLat> originRoute,List<LngAndLat> repairedList, List<LngAndLat> mappingRoad,String roadId) {

        //当前线程id
        long id = Thread.currentThread().getId();

        ThriftSend thriftSend = clients.get(id);

        if (thriftSend == null) {
            thriftSend = getThriftSend();
            clients.put(id, thriftSend);
        }

        try {
            thriftSend.openTransport();

            boolean bool = thriftSend.getClient().sendDrivingRoute(deviceId, originRoute, repairedList, mappingRoad, roadId);


        } catch (TException e) {
            logger.error("rpc 发送设备号失败");
            clients.remove(id);
            e.printStackTrace();
            return false;
        }


        return  true;
    }




}
