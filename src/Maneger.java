package com.amazonaws.samples;
//package com.mycompany.app;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
//////// ***** Imports ec2	********///////

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
//////// ***** Imports SQS	********///////

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

//////// ***** Imports S3	********///////
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

//////// ***** Imports Sqs	********///////
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.io.File;

public class Maneger {

	private static AmazonEC2 Ec2;
	private static AmazonS3 S3;
	private static AmazonSQS Sqs;
	private static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
	private static Boolean terminate = false;
	private static String name_Q_local_to_maneger = "Local_to_Maneger", Name_Q_maneger_to_Worker = "Maneger_to_Workers";
	private static int active_workers = 0, available_workers = 0, num_messages, n;
	private static Semaphore lock = new Semaphore(1);
	/////////////////////////// **** MAIN **** ///////////////////////////////////

	public static void main(String[] args) {
		start();
		printf("Create Queue Maneger To Workers .....");

		create_queue(Name_Q_maneger_to_Worker, "300");
		while (!terminate) {

			printf("try To recive Message  .....");
			Message m = recieve_message(name_Q_local_to_maneger);
			if (m != null && m.getBody() != null) {
				printf("Get A Message .....");
				analayze_the_task(m);
			}
		}

		terminate_workers();
		Delete_Queue(name_Q_local_to_maneger);
		Delete_Queue(Name_Q_maneger_to_Worker);
		Stop_maneger();
	}

	/////////////////////////// **** start basics fields ****
	/////////////////////////// ///////////////////////////////////
	private static void start() {

		Ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();

		S3 = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();

		Sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
	}

	private static void analayze_the_task(Message m) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				JSONObject newtask;
				JSONParser parser = new JSONParser();

				try {
					newtask = (JSONObject) parser.parse(m.getBody());
					switch (newtask.get("type job").toString()) {
					case "task":
						printf("message deleted");
						Delete_Message(m, name_Q_local_to_maneger); // delete the message from local_maneger queue
						do_task(newtask, m);
						break;
					case "terminate":
						terminate = true;
						Delete_Message(m, name_Q_local_to_maneger);
						break;
					}

				} catch (ParseException e) {
					// TODO Auto-generated catch block

					e.printStackTrace();
				}
			}
		}).start();

	}

	private static void do_task(JSONObject newtask, Message m) {
		String username, name_inputfile_in_bucket, name_outputfile, name_q_respone;

		username = newtask.get("username").toString();
		name_inputfile_in_bucket = newtask.get("inputfile").toString();
		name_outputfile = newtask.get("outputfile").toString();

		name_q_respone = username + name_inputfile_in_bucket;
		create_queue(name_q_respone, "120");// here we will get the messages from
		// the workers for the specific input and local
		Set_n(newtask.get("n").toString());

		int num_of_message = Distripute_input_file(name_inputfile_in_bucket, username, name_outputfile, newtask);

		int workers_created = create_start_workers(username, num_of_message);

		printf("wait to fill the summery file " + name_outputfile + " for the user" + username);
		String path_summery_file = StartToFillSummeryfile(username, name_outputfile, num_of_message, name_q_respone);

		printf("Upload the file" + name_outputfile + " for the user" + username);
		Upload_Summery_File(username, path_summery_file, name_outputfile);

		printf("Send Mwssage for the user" + username + "To get the file" + name_outputfile);
		Send_Message_for_queue(name_outputfile, username);

		try {
			lock.acquire();
			available_workers = available_workers + workers_created; // to next time when we want to create workers we
																		// know
			// that there availabl and didnt need to create
			// again !
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		lock.release();
		DeleteFile(name_outputfile);
		Delete_Queue(name_q_respone);
	}

	private static void Delete_Queue(String queue_name) {
		Sqs.deleteQueue(queue_name);
	}

	/*
	 * return the number_of_messeges that creater (operations for workers ) read the
	 * input file from the bucket that upload for it the local and send a messages
	 * (review\ task) to the queue of workers to perform
	 * 
	 */
	private static int Distripute_input_file(String name_inputfile_in_bucket, String username, String outputfile,
			JSONObject m) {
		printf("start distripute the file" + name_inputfile_in_bucket + "for the username " + username);

		m.remove("n");
		JSONObject obj;
		int count = 0;

		// This will reference one line at a time
		String line = null;

		// Always wrap FileReader in BufferedReader.
		BufferedReader bufferedReader = Get_the_text_of_file(name_inputfile_in_bucket, username);

		try {
			while ((line = bufferedReader.readLine()) != null) {

				obj = (JSONObject) new JSONParser().parse(line);
				JSONArray reviews = (JSONArray) obj.get("reviews");
				Iterator<JSONObject> i = reviews.iterator();

				while (i.hasNext()) {
					JSONObject review = (JSONObject) i.next();
					m.put("review", review.toJSONString());// add to the message this key because the workers need it
					Send_Message_for_queue(m.toJSONString(), Name_Q_maneger_to_Worker);
					count++;
				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Always close files.
		try {
			bufferedReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return count;
	}

	//////////////////////////// *** Summery File *** ////////////////////////////
	private static String StartToFillSummeryfile(String username, String name_outputfile, int num_messages_to_wait,
			String name_q_respone) {

		JSONObject Respone_message;
		JSONParser parser = new JSONParser();
		FileWriter myWriter;
		String to_write;
		try {
			// create the file using FileWriter
			myWriter = new FileWriter(name_outputfile + ".html");
			myWriter.write("<!DOCTYPE html>\r\n" + "<html>\r\n");

			while (num_messages_to_wait > 0) {
				Message m = recieve_message(name_q_respone);

				if (m != null) {
					Respone_message = (JSONObject) parser.parse(m.getBody());
					to_write = Respone_message.get("review").toString();
					if (to_write != "")
						myWriter.write(to_write);
					Delete_Message(m, name_q_respone);
					num_messages_to_wait--;
				}

			}
			myWriter.write("</body>\r\n" + "</html>");
			myWriter.close();
			// create a File linked to the same file using the name of this one;
			File f = new File(name_outputfile + ".html");
			return f.getAbsolutePath();

		} catch (ParseException | IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		printf("Successfully wrote to the " + name_outputfile + " for the user" + username);
		return null;
	}

	private static void Upload_Summery_File(String username, String path_summery_file, String name_outputfile) {
		// TODO Auto-generated method stub

		File f = new File(path_summery_file);

		try {

			// Upload a file as a new object with ContentType and title specified.
			PutObjectRequest request = new PutObjectRequest(username, name_outputfile, f);
			S3.putObject(request);

		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
	}

	/* if needed as create worker and start them to work */
	private static synchronized int create_start_workers(String username, int num_message_for_file) {

		try {
			lock.acquire();
			printf("num message before = " + num_messages);
			num_messages = num_messages + num_message_for_file;
			printf("num message after = " + num_messages);

			int needed_workers = num_messages / n; // n from the local
			printf("needed workers = " + needed_workers);

			num_messages = num_messages - needed_workers * n; // to fix the counter after we create
			// workers
			while (available_workers > 0 && needed_workers > 0) {
				needed_workers--;
				available_workers--;
			}

			lock.release();
			if (needed_workers > 0) {

				printf("* Launching Workers: ");
				TagSpecification t = new TagSpecification().withResourceType("instance")
						.withTags(new Tag().withKey("Name").withValue("Worker"));

				RunInstancesRequest req = new RunInstancesRequest("ami-097fe4bc446acfc24", needed_workers,
						needed_workers).withInstanceType(InstanceType.T2Large).withTagSpecifications(t)
								.withUserData(prepareScript());
				printf(" Created " + needed_workers + " Workers");
				Ec2.runInstances(req);
				System.out.println("Done.\n		[" + needed_workers + "]"
						+ " Workers have been launched successfully\n________________________________________________\n");
				active_workers = active_workers + needed_workers;

				Thread.currentThread();
				Thread.sleep(10000);

				return needed_workers;
			} else {
				lock.release();

				printf("Theres No need to create more workers for the username" + username);
				return 0;
			}
		} catch (InterruptedException e1) {
			lock.release();
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return 0;

	}

	//////////////////////////// *** Termination *** ////////////////////////////
	private static void terminate_workers() {

		while (available_workers < active_workers) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			printf(".");
		}
		do_terminate();

	}

	private static void do_terminate() {
		printf("Starting to terminate the workers");

		TerminateInstancesRequest terminateInstancesRequest;
		String Worker_instance_id;

		DescribeInstancesRequest request = new DescribeInstancesRequest();
		DescribeInstancesResult response = Ec2.describeInstances(request);
		for (Reservation reservation : response.getReservations()) {
			for (Instance instance : reservation.getInstances()) {
				System.out.println(instance.getInstanceId());

				if (instance.getTags() != null) {
					for (Tag tag : instance.getTags()) {
						if (tag.getKey().equals("Name") && tag.getValue().equals("Worker")
								&& instance.getState().getName().equals("running")) {
							Worker_instance_id = instance.getInstanceId();
							terminateInstancesRequest = new TerminateInstancesRequest()
									.withInstanceIds(Worker_instance_id);
							Ec2.terminateInstances(terminateInstancesRequest);
							printf("Terminate an Worker with id = " + Worker_instance_id);

						}

					}
				}
			}

		}
		return;

	}

	private static void Stop_maneger() {
		printf("Starting to stop the Maneger");

		TerminateInstancesRequest terminateInstancesRequest;
		String Maneger_instance_id;

		DescribeInstancesRequest request = new DescribeInstancesRequest();
		DescribeInstancesResult response = Ec2.describeInstances(request);
		for (Reservation reservation : response.getReservations()) {
			for (Instance instance : reservation.getInstances()) {
				System.out.println(instance.getInstanceId());

				if (instance.getTags() != null) {
					for (Tag tag : instance.getTags()) {
						if (tag.getKey().equals("Name") && tag.getValue().equals("Maneger")
								&& instance.getState().getName().equals("running")) {
							Maneger_instance_id = instance.getInstanceId();
							StopInstancesRequest requeststop = new StopInstancesRequest()
									.withInstanceIds(Maneger_instance_id);

							Ec2.stopInstances(requeststop);
						}

					}
				}
			}

		}
		return;

	}

	private static void DeleteFile(String filename) {

		File myObj = new File(filename + ".html");
		if (myObj.delete()) {
			System.out.println("Deleted the file: " + myObj.getName());
		} else {
			System.out.println("Failed to delete the file " + filename);
		}
	}

	//////////////////////////// Auxillary Functions
	////////////////////////////

	/*
	 * return a bufferreader from the text file that exist in the bucket to read it
	 * and distripute it
	 */
	private static BufferedReader Get_the_text_of_file(String Name_buckent_input_files, String username) {
		// Get an object and print its contents.
		System.out.println("Downloading an object");
		S3Object fullObject = S3.getObject(new GetObjectRequest(username, Name_buckent_input_files));
		InputStream objectData = fullObject.getObjectContent();

		return new BufferedReader(new InputStreamReader(objectData));
	}

	public static String prepareScript() {
		ArrayList<String> script = new ArrayList<String>();
		script.add("#cloud-boothook\n");
		script.add("#!/usr/bin/bash\n");
		script.add("export " + "AWS_ACCESS_KEY_ID=\"" + Get_AWS_Access_Key_Id() + "\"");
		script.add("export " + "AWS_SECRET_ACCESS_KEY=\"" + Get_AWS_Secre_tKey() + "\"");
		script.add(
				"java -jar /home/ec2-user/mohamad/Worker/my-app/target/my-app-1.0-SNAPSHOT-jar-with-dependencies.jar\n");
		script.add("echo Hi mohamad");
		String str = "";
		try {
			str = new String(Base64.encodeBase64(join(script, "\n").getBytes()), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return str;
	}

	private static String join(Collection<String> s, String delimiter) {
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			builder.append(iter.next());
			if (!iter.hasNext()) {
				break;
			}
			builder.append(delimiter);
		}
		return builder.toString();
	}

	private static void create_queue(String queue_name, String VisibilityTimeout) {

		CreateQueueRequest create_request = new CreateQueueRequest(queue_name)
				.addAttributesEntry("MessageRetentionPeriod", "86400")
				.addAttributesEntry("VisibilityTimeout", VisibilityTimeout);

		try {
			Sqs.createQueue(create_request);
		} catch (AmazonSQSException e) {
			throw e;

		}
	}

	private static Message recieve_message(String name_Q) {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(Get_Queue_Url(name_Q))
				.withMaxNumberOfMessages(1);

		List<Message> messages = Sqs.receiveMessage(receiveMessageRequest).getMessages();

		if (messages.size() > 0)
			return messages.get(0);
		else {
			try {
				Thread.currentThread();
				Thread.sleep(10000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	/* Send a specific message to the queue name */
	private static void Send_Message_for_queue(String message, String queue_name) {

		SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(Get_Queue_Url(queue_name))
				.withMessageBody(message);
		Sqs.sendMessage(send_msg_request);
	}

	private static void Delete_Message(Message m, String queue_name) {
		if (m != null)
			Sqs.deleteMessage(Get_Queue_Url(queue_name), m.getReceiptHandle());
	}

	/* return the Queue URL with the specific name */
	private static String Get_Queue_Url(String QUEUE_NAME) {
		return Sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
	}

	/* return the AWS_ACCESS_KEY_ID from the credentials */
	private static String Get_AWS_Access_Key_Id() {
		return credentialsProvider.getCredentials().getAWSAccessKeyId();
	}

	/* return the AWS_SECRET_ACCESS_KEY from the credentials */
	private static String Get_AWS_Secre_tKey() {
		return credentialsProvider.getCredentials().getAWSSecretKey();
	}

	private static void Set_n(String n1) {
		if (n == 0) {
			int review_per_wroker = Integer.parseInt(n1);
			n = review_per_wroker;

		}
	}

	private static void printf(String toprint) {
		System.out.println(toprint);
	}
}