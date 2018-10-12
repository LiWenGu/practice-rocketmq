package simple;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 可靠的同步传输 liwenguang 2018-10-11 10:33:41
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("reliable_synchronous");
        defaultMQProducer.setNamesrvAddr("localhost:9876");
        defaultMQProducer.start();
        for (int i = 0; i < 100; i++) {
            Message msg = new Message("Topic_reliable", "TagA", "这是内容".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = defaultMQProducer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        defaultMQProducer.shutdown();
    }
}
