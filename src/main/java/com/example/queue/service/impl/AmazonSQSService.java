package com.example.queue.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.example.queue.service.QueueService;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a> 
 * @since 22-Aug-2017
 */
public final class AmazonSQSService implements QueueService {

	private AmazonSQSClient sqsClient = null;

	public AmazonSQSService(AmazonSQSClient sqsClient) {
		Objects.requireNonNull(sqsClient);
		this.sqsClient = sqsClient;
	}

	@Override
	public void sendMessage(String queueUrl, String messageBody) {
		Objects.requireNonNull(queueUrl);
		Objects.requireNonNull(messageBody);
		this.sqsClient.sendMessage(queueUrl, messageBody);
	}

	@Override
	public Message recieveMessage(String queueUrl) {
		Objects.requireNonNull(queueUrl);
		ReceiveMessageRequest request = new ReceiveMessageRequest().withQueueUrl(queueUrl).withMaxNumberOfMessages(1);
		ReceiveMessageResult receiveMessageResult = this.sqsClient.receiveMessage(request);
		// As per the spec, we need to return only one message.
		return receiveMessageResult.getMessages().get(0);
	}

	@Override
	public boolean deleteMessage(String queueUrl, String reciepientHandle) {
		Objects.requireNonNull(queueUrl);
		this.sqsClient.deleteMessage(queueUrl, reciepientHandle);
		return true;
	}


	// Additional API's

	@Override
	public String createQueue(String queueUrl, ScheduledExecutorService executorService) {
		Objects.requireNonNull(queueUrl);
		CreateQueueResult result = this.sqsClient.createQueue(queueUrl);
		return result.getQueueUrl();
	}

	@Override
	public int getMessageCount(String queueUrl) {
		Objects.requireNonNull(queueUrl);
		GetQueueAttributesResult queueAttributes = sqsClient.getQueueAttributes(queueUrl,
				Arrays.asList("ApproximateNumberOfMessages"));
		return Integer.valueOf(queueAttributes.getAttributes().get("ApproximateNumberOfMessages"));
	}

	@Override
	public boolean deleteQueue(String queueUrl) {
		Objects.requireNonNull(queueUrl);
		this.sqsClient.deleteQueue(queueUrl);
		return true;
	}

	@Override
	public List<String> listQueues() {
		ListQueuesResult listQueues = this.sqsClient.listQueues();
		return listQueues.getQueueUrls();
	}
}
