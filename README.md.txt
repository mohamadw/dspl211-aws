how to run our project.
plz first of all be sure your dir to run our project as follow : 
- "login.txt" file : 		
make sure the name file is login.txt and in this file set some username in the first line without any details.
- local jar file to run
- path to this dir in the CLI or terminal.
- java -jar inputfilepath1 inputfilepath2 ... outputfilename1  outputfilename2 .... n [optional-terminate]

we use in our implementation for this assignment :
t2.larger instance type for the maneger,worker.

our assumption when run the program :  
- for every local there a uniqe username.
- there an enough memory for the worker to do the reviews that we get.
- a uniqe name for the output files.
- the n argument will create at least 1 worker for the reviews that we get 


how we implemnt this assignment ? 
1.the local upload the inputtfiles for bucket with his name (username)
2.the maneger get a message from a local in a queue "Local_to_maneger".with a "n" and "username"
3. the maneger distripute the input files make thread for every inputfile and sent the message to the queue worker
4. the maneger make a queue respone for every local with a specific inputfile
5. the worker analyze the task and send a message to the correct queue 
6. the maneger upload the file to the username bucket and sent message for the queue "username"
7. the local download the files one by one when he get a message for a specific outputfile.


our time to finish the 5 inputfiles is 17 min