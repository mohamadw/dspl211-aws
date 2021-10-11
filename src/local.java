package com.amazonaws.samples;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.AmazonServiceException;
//////// ***** Imports ec2	********///////

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
//////// ***** Imports SQS	********///////

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
//////// ***** Imports S3	********///////
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;

//////// ***** Imports Sqs	********///////
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
/*C:\Users\moham\eclipse-workspace\ass1\src\main\java\com\amazonaws\samples\0689835604.txt 
C:\Users\moham\eclipse-workspace\ass1\src\main\java\com\amazonaws\samples\B000EVOSE4.txt
C:\Users\moham\eclipse-workspace\ass1\src\main\java\com\amazonaws\samples\B001DZTJRQ.txt
C:\Users\moham\eclipse-workspace\ass1\src\main\java\com\amazonaws\samples\B0047E0EII.txt
C:\Users\moham\eclipse-workspace\ass1\src\main\java\com\amazonaws\samples\B01LYRCIPG.txt
output1 output2 output3 output4 output5 400 terminate*/
public class local {
	/*
	 * The application resides on a local (non-cloud) machine. Once started, it
	 * reads the input file from the user, and: - Checks if a Manager node is active
	 * on the EC2 cloud. If it is not, the application will start the manager node.
	 * - Uploads the file to S3. - Sends a message to an SQS queue, stating the
	 * location of the file on S3 - Checks an SQS queue for a message indicating the
	 * process is done and the response (the summary file) is available on S3. -
	 * Downloads the summary file from S3, and create an html file representing the
	 * results. - In case of terminate mode (as defined by the command-line
	 * argument), sends a termination message to the Manager. IMPORTANT: There might
	 * be more than one than one local application running at the same time.
	 */

	private static AmazonEC2 Ec2;
	private static AmazonS3 S3;
	private static AmazonSQS Sqs;
	private static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
	private static Boolean terminate = false;
	private static List<String> name_input_files = new LinkedList<String>(),
			name_output_files = new LinkedList<String>();
	private static String n, username, name_Q_local_to_maneger = "Local_to_Maneger";
	/////////////////////////// **** MAIN **** ///////////////////////////////////

	public static void main(String[] args) throws IOException {
		ReadLogin();
		printf(username);
		read_argv(args);

		start();

		printf("... Check if a Manager node is active on the EC2 cloud...");
		maneger_node();

		printf("... Start to Create a Bucket...");
		create_bucket(username);

		printf("... Start to Upload The Files...");
		upload_files(username);

		printf("... Start to Create a Queue Local To Maneger...");
		create_queue(name_Q_local_to_maneger);

		printf("... Start to Create a Queue...");
		create_queue(username);// the respone queue <get the message from maneger

		printf("... Send a Message to Maneger...");
		send_messages(username);

		printf("... Start Wait The Maneger ...");
		Wait_Response_from_Maneger();

		Send_Terminate();
		Delete_Queue(username);
		
		Delete_Bucket();
		System.out.println("Game Over!");
	}



	private static void send_messages(String username2) {
		// TODO Auto-generated method stub
		String message;
		for (int i = 0; i < name_input_files.size(); i++) {
			message = make_message("task", username, "inputfile" + (i + 1), name_output_files.get(i), n);
			send_message(username, message);

		}

	}

	/////////////////////////// **** start basics fields ****
	/////////////////////////// ///////////////////////////////////
	private static void start() {

		Ec2 = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();

		S3 = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();

		Sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion("us-east-1").build();
	}

	/////////////////////////// **** Maneger Node ****
	/////////////////////////// ///////////////////////////////////

	/*
	 * Checks if a Manager node is active on the EC2 cloud. If it is not, the
	 * application will start the manager node.
	 */
	private static void maneger_node() throws UnsupportedEncodingException {

		StartInstancesRequest startInstancesRequest;
		String maneger_instance_id;

		DescribeInstancesRequest request = new DescribeInstancesRequest();
		DescribeInstancesResult response = Ec2.describeInstances(request);
		for (Reservation reservation : response.getReservations()) {
			for (Instance instance : reservation.getInstances()) {
				if (instance.getTags() != null) {
					for (Tag tag : instance.getTags()) {
						if (tag.getKey().equals("Maneger")) {
							switch (instance.getState().getName()) {
							case "running":
								return;
							case "stopping":
								maneger_instance_id = instance.getInstanceId();
								startInstancesRequest = new StartInstancesRequest()
										.withInstanceIds(maneger_instance_id);

								Ec2.startInstances(startInstancesRequest);
								return;
							case "pending":
								int i = 15;
								while (i > 0) {
									try {
										printf(".");

										TimeUnit.SECONDS.sleep(1);

									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
									i--;
								}
								return;

							}

						}
					}
				}
			}
		}

		/* CREATE A MANEGER NODE */

		printf("... Create A MAneger Node For you ...");
		runInstance();
		return;
	}

	/**/
	private static void upload_files(String bucket_name) {
		int i = 1;
		String object_name;
		ArrayList<File> files = new ArrayList<File>();
		for (String path : name_input_files) {
			files.add(new File(path));
		}

		try {
			for (File f : files) {

				// Upload a file as a new object with ContentType and title specified.
				object_name = "inputfile" + i;
				PutObjectRequest request = new PutObjectRequest(bucket_name, object_name, f);

				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentType(name_output_files.get(i - 1));
				request.setMetadata(metadata);
				S3.putObject(request);
				i++;
			}
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

	private static void Wait_Response_from_Maneger() {

		List<Message> messages;
		int i = name_input_files.size();
		while (i > 0) {
			printf("Try to recive message from maneger");
			messages = recieve_message(username); // recive the message from the response queue for us
			if (messages != null) {
				for (Message m : messages) {

					WriteFile(m.getBody());
					Delete_Message(m, username);

					printf("We Get a Message From maneger ");

				}
				i--;
			}
		}
	}

	private static void WriteFile(String name_outputfile) {

		FileWriter myWriter;
		String line;
		BufferedReader reader = DownlaodFile_FromBucket(name_outputfile);
		try {
			// create the file using FileWriter
			myWriter = new FileWriter(name_outputfile + ".html");

			while ((line = reader.readLine()) != null) {
				myWriter.write(line);
			}
			myWriter.close();

		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		printf("Successfully wrote to the " + name_outputfile);
	}

	/*
	 * return a bufferreader from the text file that exist in the bucket to read it
	 * and distripute it
	 */
	private static BufferedReader DownlaodFile_FromBucket(String name_outputfile_in_bucket) {
		// Get an object and print its contents.
		System.out.println("Start Downloading the " + name_outputfile_in_bucket + "file");
		S3Object fullObject = S3.getObject(new GetObjectRequest(username, name_outputfile_in_bucket));
		InputStream objectData = fullObject.getObjectContent();

		return new BufferedReader(new InputStreamReader(objectData));
	}

	private static void create_bucket(String bucket_name) {

		try {
			S3.createBucket(bucket_name);
		} catch (AmazonS3Exception e) {
			System.err.println(e.getErrorMessage());
		}
	}

	private static void read_argv(String[] args) {
		int argc, num_files;

		argc = args.length;
		if (argc < 3)
			throw new IllegalArgumentException("Illegal Number Of Arguments");

		if (args[argc - 1].equals("terminate")) {
			terminate = true;
			n = args[argc - 2];
			num_files = (argc - 2) / 2;
			
		} else {
			terminate = false;
			n = args[argc - 1];
			num_files = (argc - 1) / 2;
		}

		for (int i = 0; i < num_files; i++) { /// Save the argv of name files in the lists to use them later
			name_input_files.add(args[i]);
			name_output_files.add(args[i + num_files]);
		}
	}

	public static String prepareScript() throws UnsupportedEncodingException {
		ArrayList<String> script = new ArrayList<String>();
		script.add("#cloud-boothook\n");
		script.add("#!/usr/bin/bash\n");
		script.add("export " + "AWS_ACCESS_KEY_ID=\"" + Get_AWS_Access_Key_Id() + "\"");
		script.add("export " + "AWS_SECRET_ACCESS_KEY=\"" + Get_AWS_Secre_tKey() + "\"");
		script.add(
				"java -jar /home/ec2-user/mohamad/demo/my-app/target/my-app-1.0-SNAPSHOT-jar-with-dependencies.jar\n");
		script.add("echo Hi mohamad");

		String str = new String(Base64.encodeBase64(join(script, "\n").getBytes()), "UTF-8");
		return str;
	}

	public static void runInstance() throws UnsupportedEncodingException {
		TagSpecification t = new TagSpecification().withResourceType("instance")
				.withTags(new Tag().withKey("Name").withValue("Maneger"));
		String userdata = prepareScript();
		RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

		runInstancesRequest.withImageId("ami-08efa777fffa86ca7").withInstanceType(InstanceType.T2Large).withMinCount(1)
				.withMaxCount(1).withKeyName("my_key1").withUserData(userdata).withTagSpecifications(t);
		Ec2.runInstances(runInstancesRequest);
	}

	private static void create_queue(String username) {

		CreateQueueRequest create_request = new CreateQueueRequest(username).addAttributesEntry("DelaySeconds", "1")
				.addAttributesEntry("MessageRetentionPeriod", "86400");

		try {
			Sqs.createQueue(create_request);
		} catch (AmazonSQSException e) {
			if (!e.getErrorCode().equals("QueueAlreadyExists")) {
				throw e;
			} else {
				printf("The Queue Alreade Exist (LOcal To MAnegar)");
			}
		}
	}

	private static void send_message(String username, String message) {

		SendMessageRequest send_msg_request = new SendMessageRequest()
				.withQueueUrl(Get_Queue_Url(name_Q_local_to_maneger)).withMessageBody(message);
		Sqs.sendMessage(send_msg_request);
	}

	private static String make_message(String type_job, String username, String name_inputfile, String name_outputfile,
			String n) {
		JSONObject message = new JSONObject();

		switch (type_job) {
		case "task":
			message.put("type job", type_job);
			message.put("username", username);
			message.put("inputfile", name_inputfile);
			message.put("outputfile", name_outputfile);
			message.put("n", n);
			break;

		case "terminate":
			message.put("type job", type_job);
			message.put("username", username);
			break;
		}
		return message.toJSONString();
	}

	private static List<Message> recieve_message(String name_Q) {

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest().withQueueUrl(Get_Queue_Url(name_Q));

		List<Message> messages = Sqs.receiveMessage(receiveMessageRequest).getMessages();

		if (messages.size() > 0)
			return messages;
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

	private static void Delete_Message(Message m, String queue_name) {
		if (m != null)
			Sqs.deleteMessage(Get_Queue_Url(queue_name), m.getReceiptHandle());
	}

	private static void Send_Terminate() {
		if (terminate) {

			String message;
			message = make_message("terminate", username, "", "", "");
			send_message(username, message);
		}
	}
	
	private static void Delete_Bucket() {
		System.out.println(" - removing objects from bucket");
		ObjectListing object_listing = S3.listObjects(username);
		while (true) {
		    for (Iterator<?> iterator =
		         object_listing.getObjectSummaries().iterator();
		         iterator.hasNext(); ) {
		        S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
		        S3.deleteObject(username, summary.getKey());
		    }

		    // more object_listing to retrieve?
		    if (object_listing.isTruncated()) {
		        object_listing = S3.listNextBatchOfObjects(object_listing);
		    } else {
		        break;
		    }
		}
		
		System.out.println(" OK, bucket ready to delete!");
		S3.deleteBucket(username);
		
	}

	//////////////////////////// @@@@@@@@@@@ Auxillary Functions
	//////////////////////////// @@@@@@@@@@@@@@@@@@@@@@@@@
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

	private static void printf(String toprint) {
		System.out.println(toprint);
	}

	private static void Delete_Queue(String queue_name) {
		Sqs.deleteQueue(queue_name);
	}

	/**
	 * Assumption: the user will use this correctly 1- the first line will be the
	 * name of the user
	 * 
	 **/
	private static void ReadLogin() throws IOException {
		File file = new File(".\\login.txt");

		BufferedReader br = new BufferedReader(new FileReader(file));
		username = br.readLine();
		br.close();
	}
}