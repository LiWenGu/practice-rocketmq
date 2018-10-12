package batch;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ListSplitter implements Iterator<List<Message>> {

    private final int SIZE_LIMIT = 1000 * 1000;
    private final List<Message> messageList;
    private int currIndex;

    public ListSplitter(List<Message> messageList) {
        this.messageList = messageList;
    }

    public boolean hasNext() {
        return currIndex < messageList.size();
    }

    public List<Message> next() {
        int nextIndex = currIndex;
        int totalSize = 0;
        for (; nextIndex < messageList.size(); nextIndex++) {
            Message message = messageList.get(nextIndex);
            int tmpSize = message.getTopic().length() + message.getBody().length;
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            tmpSize = tmpSize + 20; //for log overhead
            if (tmpSize > SIZE_LIMIT) {
                if (nextIndex - currIndex == 0) {
                    nextIndex++;
                }
                break;
            }
            if (tmpSize + totalSize > SIZE_LIMIT) {
                break;
            } else {
                totalSize += tmpSize;

            }
        }
        List<Message> subList = messageList.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }

    public void remove() {

    }

}
