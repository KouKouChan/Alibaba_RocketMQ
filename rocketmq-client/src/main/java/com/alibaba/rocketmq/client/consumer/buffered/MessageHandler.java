package com.alibaba.rocketmq.client.consumer.buffered;

import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * This interface defines how business unit processes each messages. It's assumed to be implemented by business
 * developer.
 */
public abstract class MessageHandler {

    /**
     * Defines topic this handle subscribes. This field is strictly required. It cannot be null or empty.
     */
    private String topic;

    /**
     * Tags for simple message filtering.
     */
    private String tag;

    /**
     * User define processing logic, implemented by ultimate business developer.
     * @param message Message to process.
     * @return 0 if business logic has already properly consumed this message; positive int N if this message is
     * supposed to be consumed again N milliseconds later.
     */
    public abstract int handle(MessageExt message);


    /**
     * Getter of name-sake field.
     * @return value of name-sake field.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Setter of name-sake field.
     *
     * <strong>Setting this field is strictly required.</strong>
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Getter of name-sake field.
     * @return value of name-sake field.
     */
    public String getTag() {
        return tag;
    }

    /**
     * Setter of name-sake field.
     */
    public void setTag(String tag) {
        this.tag = tag;
    }
}
