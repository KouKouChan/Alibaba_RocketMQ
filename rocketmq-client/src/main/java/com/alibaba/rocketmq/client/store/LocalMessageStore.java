package com.alibaba.rocketmq.client.store;

import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import java.io.IOException;

public interface LocalMessageStore {

    /**
     * Stash a new message.
     * @param message Message to stash.
     */
    boolean stash(Message message);

    /**
     * This method returns numbers of messages stashed.
     * @return number of messages stashed.
     */
    int getNumberOfMessageStashed();

    /**
     * This method would pop out at most <code>n</code> messages from local store.
     * @param n Number of messages assumed to be popped out.
     * @return Array of messages.
     */
    MessageExt[] pop(int n);

    void start() throws IOException;

    void close();
}
