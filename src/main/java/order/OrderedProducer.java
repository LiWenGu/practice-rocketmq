package order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class OrderedProducer {

    public static void main(String[] args) throws Exception {
        MQProducer producer = new DefaultMQProducer("order_group_name");
        ((DefaultMQProducer) producer).setNamesrvAddr("localhost:9876");
        producer.start();
        String[] tags = new String[] {"TagA", "TagC", "TagD"};
        for (int i = 0; i < 100; i++) {
            int orderId = i % 10;
            Message msg = new Message("orderTests", tags[i % tags.length],
                    "KEY" + i, ("hello" + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
