import com.muheda.thrift.produce.DrivingRouteData;
import com.muheda.thrift.produce.LngAndLat;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ThriftTest {


    @Test
    public  void test()  {


        TFramedTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9999, 10000));

        TBinaryProtocol protocol = new TBinaryProtocol(transport);

        DrivingRouteData.Client client = new DrivingRouteData.Client(protocol);

        try {
            transport.open();
        } catch (TTransportException e) {
            e.printStackTrace();
        }


        if(transport.isOpen()){


            String deviceId  = "123";

            List<LngAndLat> list = new ArrayList<>();

            LngAndLat point = new LngAndLat("deviceId",10.0, 12.2, "hello");

            list.add(point);

            //String deviceId, List<produce.LngAndLat> originRoute, List<LngAndLat> repairedList, List<LngAndLat> mappingRoad, String roadId)

            try {


                long l = System.currentTimeMillis();


                for (int i = 0; i < 10000; i++) {

                    boolean roadId = client.sendDrivingRoute(deviceId, list, list, list, "roadId");

                }

                System.out.println(System.currentTimeMillis() - l);


            } catch (TException e) {

                e.printStackTrace();

            }


            try {
                client.ping();
            } catch (TException e) {
                e.printStackTrace();
            }



        }



    }
}
