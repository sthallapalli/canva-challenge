package com.example.queue.service.impl;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;
import com.example.queue.MessageQueue;
import com.example.queue.service.QueueService;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a> 
 * @since 23-Aug-2017
 */
public final class InMemoryQueueService implements QueueService {

	private static final Logger LOG = Logger.getLogger(InMemoryQueueService.class.getName());

	private MessageQueueServiceHelper<LinkedBlockingDeque<Message>> serviceHelper = null;
	private ConcurrentMap<String, MessageQueue<LinkedBlockingDeque<Message>>> queues = null;

	public InMemoryQueueService(ConcurrentMap<String, MessageQueue<LinkedBlockingDeque<Message>>> queues) {
		this.queues = queues;
		this.serviceHelper = new MessageQueueServiceHelper<>(queues);
	}

	public void setMessageQueueHelper(MessageQueueServiceHelper<LinkedBlockingDeque<Message>> helper) {
		this.serviceHelper = helper;
	}

	@Override
	public void sendMessage(String queueUrl, String messageBody) {
		this.serviceHelper.sendMessage(queueUrl, messageBody);
	}

	@Override
	public Message recieveMessage(String queueUrl) {
		return this.serviceHelper.recieveMessage(queueUrl);
	}

	@Override
	public boolean deleteMessage(String queueUrl, String receiptHandle) {
		return this.serviceHelper.deleteMessage(queueUrl, receiptHandle);
	}

	
	//Additional API's
	
	@Override
	public String createQueue(String queueUrl, ScheduledExecutorService executorService) {
		Objects.requireNonNull(queueUrl);
		MessageQueue<LinkedBlockingDeque<Message>> queue = this.serviceHelper.getQueue(queueUrl);
		if (queue != null) {
			LOG.log(Level.INFO, "Queue with queueUrl [" + queueUrl + "] is already exists.");
			return queueUrl;
		}
		this.queues.put(queueUrl,
				new MessageQueue<LinkedBlockingDeque<Message>>(new LinkedBlockingDeque<>(), executorService));
		return queueUrl;
	}
	
	@Override
	public int getMessageCount(String queueUrl) {
		return this.serviceHelper.getMessageCount(queueUrl);
	}

	@Override
	public boolean deleteQueue(String queueUrl) {
		return this.serviceHelper.deleteQueue(queueUrl);
	}

	@Override
	public List<String> listQueues() {
		return this.serviceHelper.listQueues();
	}
}
