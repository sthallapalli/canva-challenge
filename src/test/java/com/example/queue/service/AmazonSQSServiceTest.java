package com.example.queue.service;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.example.queue.service.impl.AmazonSQSService;

/**
 * @author <a href="mailto:sthallapalli@outlook.com">sthallapalli</a>
 * @since 22-Aug-2017
 */

@RunWith(MockitoJUnitRunner.class)
public class AmazonSQSServiceTest {

	@Mock
	private AmazonSQSClient amazonSQSClient;

	private QueueService amazonQueueService;

	@Before
	public void init() {
		this.amazonQueueService = new AmazonSQSService(amazonSQSClient);
	}

	@Test
	public void testSendMessage() {
		when(this.amazonSQSClient.sendMessage(anyString(), anyString())).thenReturn(mock(SendMessageResult.class));
		this.amazonQueueService.sendMessage(anyString(), anyString());
		verify(this.amazonSQSClient, times(1)).sendMessage(anyString(), anyString());
	}

	@Test
	public void testRecieveMessage() {

		Message message = new Message();
		message.setBody("Message Body");

		List<Message> messages = new ArrayList<>();
		messages.add(message);

		ReceiveMessageResult result = new ReceiveMessageResult();
		result.setMessages(messages);

		when(this.amazonSQSClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);

		Message msg = this.amazonQueueService.recieveMessage(anyString());
		verify(this.amazonSQSClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
		Assert.assertNotNull(msg);
		Assert.assertEquals(msg.getBody(), "Message Body");
	}

	@Test
	public void testDeleteMessage() {
		doNothing().when(this.amazonSQSClient).deleteMessage(anyString(), anyString());
		this.amazonQueueService.deleteMessage(anyString(), anyString());
		verify(this.amazonSQSClient, times(1)).deleteMessage(anyString(), anyString());
	}

	// Additional API's

	@Test
	public void testMessageCount() {
		GetQueueAttributesResult result = new GetQueueAttributesResult();
		result.addAttributesEntry("ApproximateNumberOfMessages", "10");
		List<String> attrs = Arrays.asList("ApproximateNumberOfMessages");
		when(this.amazonSQSClient.getQueueAttributes(anyString(), eq(attrs))).thenReturn(result);
		int count = this.amazonQueueService.getMessageCount("queue1");
		Assert.assertEquals(count, 10);
	}

	@Test
	public void listQueues() {
		ListQueuesResult result = new ListQueuesResult();
		List<String> queues = new ArrayList<>();
		queues.add("queue1");
		result.setQueueUrls(queues);

		when(this.amazonSQSClient.listQueues()).thenReturn(result);
		List<String> list = this.amazonQueueService.listQueues();
		verify(this.amazonSQSClient, times(1)).listQueues();
		Assert.assertNotNull(list);
	}
}
