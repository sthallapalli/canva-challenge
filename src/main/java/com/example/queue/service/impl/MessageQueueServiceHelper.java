package com.example.queue.service.impl;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.example.queue.MessageQueue;
import com.google.common.collect.Lists;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a>
 * @since 25-Aug-2017
 */
public class MessageQueueServiceHelper<T extends BlockingDeque<Message>> {

	private static final Logger LOG = Logger.getLogger(MessageQueueServiceHelper.class.getName());

	private ConcurrentMap<String, MessageQueue<T>> queues;

	public MessageQueueServiceHelper(ConcurrentMap<String, MessageQueue<T>> queues) {
		this.queues = queues;
	}

	public void sendMessage(String queueUrl, String messageBody) {
		this.getQueue(queueUrl).push(prepareMessage(messageBody));
	}

	public Message recieveMessage(String queueUrl) {
		return this.getQueue(queueUrl).poll();
	}

	public boolean deleteMessage(String queueUrl, String reciepientHandle) {
		return this.getQueue(queueUrl).delete(reciepientHandle);
	}

	// Additional API's
	public int getMessageCount(String queueUrl) {
		return this.getQueue(queueUrl).size();
	}

	public List<String> listQueues() {
		return Lists.newArrayList(this.queues.keySet());
	}

	public MessageQueue<T> getQueue(final String queueUrl) {
		MessageQueue<T> queue = this.queues.get(queueUrl);
		if (queue == null) {
			LOG.log(Level.SEVERE, "Queue [" + queueUrl + "] does not exists.");
			throw new QueueDoesNotExistException("Queue [" + queueUrl + "] does not exists.");
		}
		return queue;
	}

	private Message prepareMessage(String messageBody) {
		Message message = new Message();
		message.setBody(String.valueOf(messageBody));
		String randomUuid = UUID.randomUUID().toString();
		message.setMessageId(randomUuid);
		message.setReceiptHandle(randomUuid);
		return message;
	}
}
