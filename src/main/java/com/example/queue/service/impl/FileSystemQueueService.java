package com.example.queue.service.impl;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;
import com.example.queue.FileQueue;
import com.example.queue.MessageQueue;
import com.example.queue.service.QueueService;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a> 
 * @since 24-Aug-2017
 */
public final class FileSystemQueueService implements QueueService {

	private static final Logger LOG = Logger.getLogger(FileSystemQueueService.class.getName());

	private MessageQueueServiceHelper<FileQueue<Message>> serviceHelper = null;
	private ConcurrentMap<String, MessageQueue<FileQueue<Message>>> queues = null;

	public FileSystemQueueService(ConcurrentMap<String, MessageQueue<FileQueue<Message>>> queues) {
		this.queues = queues;
		this.serviceHelper = new MessageQueueServiceHelper<>(queues);
	}

	public void setMessageQueueHelper(MessageQueueServiceHelper<FileQueue<Message>> helper) {
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
	public boolean deleteMessage(String queueUrl, String reciepientHandle) {
		return this.serviceHelper.deleteMessage(queueUrl, reciepientHandle);
	}


	//Additional API's
	
	@Override
	public String createQueue(String queueUrl, ScheduledExecutorService executorService) {
		Objects.requireNonNull(queueUrl);
		MessageQueue<FileQueue<Message>> queue = this.serviceHelper.getQueue(queueUrl);
		if (queue != null) {
			LOG.log(Level.INFO, "Queue with queueUrl [" + queueUrl + "] is already exists.");
			return queueUrl;
		}
		this.queues.put(queueUrl, new MessageQueue<>(new FileQueue<Message>(queueUrl), null));
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
