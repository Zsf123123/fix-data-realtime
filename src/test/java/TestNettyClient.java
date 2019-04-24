import com.muheda.domain.Msg;
import com.muheda.netty.NettyClient;
import org.junit.Test;

public class TestNettyClient {


    private static NettyClient nettyClient = new NettyClient(9999);


    static {

        nettyClient.start();
    }

  /*  @Before
    public void pre() throws Exception {

        nettyClient = new NettyClient(9999);
        nettyClient.start();

    }*/



    @Test
    public void testNetty() throws InterruptedException {

        for (int i = 0; i < 100000; i++) {

//            nettyClient.sendDataToServer(String.valueOf(i));
        }


    }

}
