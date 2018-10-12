package scheduled;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class ScheduledMessageProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        int totalMessagesToSend = 100;
        System.out.println(System.currentTimeMillis());
        for (int i = 0; i < totalMessagesToSend; i++){
            Message message = new Message("ScheduledMessageConsumerTopic", ("Hello scheduled message " + i).getBytes());
            message.setDelayTimeLevel(3);
            producer.send(message);
        }
        producer.shutdown();
    }
}
