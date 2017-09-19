package com.example.queue.service;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.amazonaws.services.sqs.model.Message;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a> 
 * @since 23-Aug-2017
 */

public interface QueueService {

	/**
	 * Pushes the message to specified queueUrl. 
	 * @param queueUrl
	 * @param messageBody
	 */
	public void sendMessage(String queueUrl, String messageBody);
	
	
	/**
	 * Gets the messages from specified queueUrl.
	 * @param queueUrl
	 * @return Message
	 */
	public Message recieveMessage(String queueUrl);
	
	
	/**
	 * Delete the message with receiptHandle in specified queueUrl
	 * @param queueUrl
	 * @param receiptHandle
	 */
	public boolean deleteMessage(String queueUrl, String receiptHandle);
	
	
	// Additional Api's 
	
	/**
	 * Creates the queue with specified queueUrl. The scheduler service for Amazon SQS Service does not make any sense.
	 * No need to pass scheduler service.
	 * @param queueUrl
	 * @param executorService, please pass null for Amazon SQS service.
	 * @return QueueName
	 */
	public String createQueue(String queueUrl, ScheduledExecutorService executorService);

	/**
	 * Gets the message count on specified queueUrl
	 * @param queueUrl
	 * @return message count
	 */
	public int getMessageCount(String queueUrl);
	
	/**
	 * Deletes the queue by queueUrl
	 * @param queueUrl
	 * @return
	 */
	public boolean deleteQueue(String queueUrl);
	
	
	/**
	 * Lists the queues
	 * @return queueList
	 */
	public List<String> listQueues();
}
