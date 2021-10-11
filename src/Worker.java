package com.amazonaws.samples;
//package com.mycompany.app;

import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Worker {

	private static AmazonSQS Sqs;
	private static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();

	final static sentimentAnalysisHandler sentimentAnalysisHandler = new sentimentAnalysisHandler();
	final static namedEntityRecognitionHandler namedEntityRecognitionHandler = new namedEntityRecognitionHandler();
	private static String output = "";
	private static String Namw_Q_maneger_to_Worker = "Maneger_to_Workers";
	private static String URL_Q_maneger_to_Worker;

	public static void main(String[] args) throws Exception {

		System.out.println("...      Worker Started      ...");

		start();

		int i = 0;
		JSONParser parser = new JSONParser();
		JSONObject newtask;
		while (true) {
			Message m = recieve_message_from_maneger();
			output = "";
			if (m != null) {
				try {
					newtask = (JSONObject) parser.parse(m.getBody());
					do_task(newtask);
					printf("finished analyze the task...");

					Delete_Message(m, Namw_Q_maneger_to_Worker);
					printf("The message deleted...");

					String response_queue = newtask.get("username").toString() + newtask.get("inputfile").toString();

					Send_Message(newtask, response_queue);
					printf("The message sent...");

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				i++;
				System.out.println(i);

			}

		}

	}

	/*
	 * analyze the review and color it <in addition convert it to html string
	 * 
	 */
	private static void do_task(JSONObject newtask) {

		try {
			printf("start doing the task");
			JSONParser parser = new JSONParser();
			JSONObject review = (JSONObject) parser.parse(newtask.get("review").toString());
			int result_person = 0, result_algo;
			///// check sarcasm //
			String entity = "[]";
			String[] rating_and_entity = namedEntityRecognitionHandler.printEntities(review.toString());

			result_algo = sentimentAnalysisHandler.findSentiment(review.toString());

			if (rating_and_entity[0] != "") {
				result_person = Integer.parseInt(rating_and_entity[0]);
				entity = rating_and_entity[1];
			}

			if (result_algo - result_person == 0) {
				// not sarcasm

				switch (result_algo) {
				case 1:
					output = output + "<p style=\"color:darkred\">" + entity + "	" + "	sarcasm? No		" + "link: "
							+ review.get("link").toString() + ".</p>\r\n";
					break;
				case 2:
					output = output + "<p style=\"color:red\">	" + entity + "	" + "	sarcasm? No		" + "link: "
							+ review.get("link").toString() + ".</p>\r\n";
					break;

				case 3:
					output = output + "<p style=\"color:black\">" + entity + "	" + "	sarcasm? No		" + "link: "
							+ review.get("link").toString() + ".</p>\r\n";
					break;

				case 4:
					output = output + "<p style=\"color:lightgreen\">" + entity + "	" + "	sarcasm? No		" + "link: "
							+ review.get("link").toString() + ".</p>\r\n";
					break;

				case 5:
					output = output + "<p style=\"color:darkgreen\">" + entity + "	" + "	sarcasm? No		" + "link: "
							+ review.get("link").toString() + ".</p>\r\n";
					break;
				}
			}

			else {
				switch (result_algo) {
				case 1:
					output = output + "<p style=\"color:darkred\">" + entity + "	" + "	sarcasm? Yes		"
							+ "link: " + review.get("link").toString() + ".</p>\r\n";
					break;
				case 2:
					output = output + "<p style=\"color:red\">	" + entity + "	" + "	sarcasm? Yes		" + "link: "
							+ review.get("link").toString() + ".</p>\r\n";
					break;

				case 3:
					output = output + "<p style=\"color:black\">" + entity + "	" + "	sarcasm? Yes		" + "link: "
							+ review.get("link").toString() + ".</p>\r\n";
					break;

				case 4:
					output = output + "<p style=\"color:lightgreen\">" + entity + "	" + "	sarcasm? Yes		"
							+ "link: " + review.get("link").toString() + ".</p>\r\n";
					break;

				case 5:
					output = output + "<p style=\"color:darkgreen\">" + entity + "	" + "	sarcasm? Yes		"
							+ "link: " + review.get("link").toString() + ".</p>\r\n";
					break;
				}
			}

			printf(output);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/////////////////////////// **** start basics fields ****
	/////////////////////////// ///////////////////////////////////

	private static void start() {
		Sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
		URL_Q_maneger_to_Worker = Get_Queue_Url(Namw_Q_maneger_to_Worker);
	}

	private static Message recieve_message_from_maneger() {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(URL_Q_maneger_to_Worker)
				.withMaxNumberOfMessages(1);

		List<Message> messages = Sqs.receiveMessage(receiveMessageRequest).getMessages();

		if (messages.size() > 0)
			return messages.get(0);
		else {
			try {
				Thread.sleep(5000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	/*
	 * Send a specific message to the queue name
	 */
	private static void Send_Message(JSONObject message, String send_to) {

		message.remove("review");
		message.put("review", output);

		SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(Get_Queue_Url(send_to))
				.withMessageBody(message.toJSONString());
		Sqs.sendMessage(send_msg_request);
	}

	private static void Delete_Message(Message m, String delete_from) {
		if (m != null)
			Sqs.deleteMessage(Get_Queue_Url(delete_from), m.getReceiptHandle());
	}

	//////////////////////////// @@@@@@@@@@@ Auxillary Functions
	//////////////////////////// @@@@@@@@@@@@@@@@@@@@@@@@@

	/* return the Queue URL with the specific name */
	private static String Get_Queue_Url(String QUEUE_NAME) {
		return Sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
	}

	private static void printf(String toprint) {
		System.out.println(toprint);
	}

}