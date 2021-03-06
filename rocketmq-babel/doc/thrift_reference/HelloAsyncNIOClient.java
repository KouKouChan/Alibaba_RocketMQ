import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;

import java.io.IOException;

public class HelloAsyncNIOClient {
    private static final int PORT = Integer.parseInt(System.getProperty("RocketMQProducerPort", "3210"));
    public static void main(String[] args) throws TException, IOException, InterruptedException {
        for (int i = 0; i < 65536; i++) {
            TNonblockingSocket transport = new TNonblockingSocket("localhost", PORT);
            transport.startConnect();
            while (!transport.finishConnect()) {
                Thread.sleep(100);
            }

            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Hello.Client client = new Hello.Client(protocol);

            System.out.println(client.echo("Msg: "));

            transport.close();
        }


    }
}
