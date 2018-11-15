/**
 * Copyright: Copyright 2016-2020 JD.COM All Right Reserved
 * FileName: bftsmart.tom.ReplyContextMessage
 * Author: shaozhuguang
 * Department: Y事业部
 * Date: 2018/11/13 下午3:27
 * Description:
 */
package bftsmart.tom;

import bftsmart.tom.core.messages.TOMMessage;

/**
 *
 * @author shaozhuguang
 * @create 2018/11/13
 * @since 1.0.0
 */

public class ReplyContextMessage {

    private byte[] reply;

    private MessageContext messageContext;

    private ReplyContext replyContext;

    private TOMMessage tomMessage;

    public ReplyContextMessage(ReplyContext replyContext, TOMMessage tomMessage) {
        this.replyContext = replyContext;
        this.tomMessage = tomMessage;
    }

    public ReplyContext getReplyContext() {
        return replyContext;
    }

    public void setReplyContext(ReplyContext replyContext) {
        this.replyContext = replyContext;
    }

    public TOMMessage getTomMessage() {
        return tomMessage;
    }

    public void setTomMessage(TOMMessage tomMessage) {
        this.tomMessage = tomMessage;
    }

    public byte[] getReply() {
        return reply;
    }

    public void setReply(byte[] reply) {
        this.reply = reply;
    }

    public MessageContext getMessageContext() {
        return messageContext;
    }

    public void setMessageContext(MessageContext messageContext) {
        this.messageContext = messageContext;
    }
}