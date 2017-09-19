package com.example.queue.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;
import com.example.queue.FileQueue;
import com.example.queue.MessageQueue;
import com.example.queue.service.QueueService;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a>
 * @since 24-Aug-2017
 */
public final class FileQueueService implements QueueService {

	private static final Logger LOG = Logger.getLogger(FileQueueService.class.getName());

	private MessageQueueServiceHelper<FileQueue<Message>> serviceHelper = null;
	private ConcurrentMap<String, MessageQueue<FileQueue<Message>>> queues = null;

	public FileQueueService(ConcurrentMap<String, MessageQueue<FileQueue<Message>>> queues) {
		this.queues = queues;
		this.serviceHelper = new MessageQueueServiceHelper<>(queues);
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

	// Additional API's

	@Override
	public int getMessageCount(String queueUrl) {
		return this.serviceHelper.getMessageCount(queueUrl);
	}

	@Override
	public List<String> listQueues() {
		return this.serviceHelper.listQueues();
	}

	public void setMessageQueueHelper(MessageQueueServiceHelper<FileQueue<Message>> helper) {
		this.serviceHelper = helper;
	}
}
