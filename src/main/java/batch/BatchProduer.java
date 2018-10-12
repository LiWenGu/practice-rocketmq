package batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

public class BatchProduer {
    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProduer");
        producer.setNamesrvAddr("localhost:9876");
        String topic = "BatchTestTopic";
        List<Message> messageList = new ArrayList<Message>();
        messageList.add(new Message(topic, "TagA", "Order001", "hello".getBytes()));
        messageList.add(new Message(topic, "TagA", "Order001", "hello".getBytes()));
        messageList.add(new Message(topic, "TagA", "Order001", "hello".getBytes()));
        try {
            producer.send(messageList);
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
