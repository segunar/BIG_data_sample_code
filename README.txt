*
******************************************************************************
* 			
* SET UP THE ENVIRONMENT FOR RUNNING MAP REDUCE JOBS		
*			
******************************************************************************
*
******************************************************************************
*
* 1. Boot up the VM in which we are going to work with Hadoop 
*
******************************************************************************
*
1.1. Windows --> Open VMware Workstation and boot up the VM 'Cloudera-quickstart-vm-5.5.0'
*
******************************************************************************
*
* 2. Bring the DataSet, the MapReduce Python Code and the Command Shell script to the VM 
*
******************************************************************************
*
2.1. VMware --> From the VM desktop, open the folder 'cloudera's home'.
*
2.2. Windows --> Open the folder of the example you want to try. For example: 
		 Average Length of Words
*
2.3. Windows to VMware --> Copy the following folders and files from Windows to VMware 'cloudera's home':
     - Folder: my_dataset
     - Folder: my_python_mapreduce
     - File: my_commands.sh
*
******************************************************************************
*
* 3. Run the Map Reduce job in Hadoop  
*
******************************************************************************
*
3.1. VMware --> Open a terminal by clicking on the desktop icon
*
3.2. VMware --> Make the script file 'my_commands.sh' executable:
		In the terminal, type: chmod u+x my_commands.sh
*
3.3. VMware --> Run the script triggering the Hadoop Map Reduce job:
		In the terminal, type: ./my_commands.sh
*
******************************************************************************
*
* 4. Bring the solution of the MapReduce job back to Windows  
*
******************************************************************************
*
4.1. VMware --> If your MapReduce job succeeds, then the script informs you of it and creates a new subfolder 'my_result' at the folder 'cloudera's home'.
     Move this folder back to Windows.
*
******************************************************************************
*
* 5. Close the terminal and remove the subfolders  
*
******************************************************************************
*
5.1. VMware --> Close the terminal
*
5.2. VMware --> Delete the subfolders 'my_dataset', 'my_python_mapreduce' and 'my_result' from 'cloudera's home',
     so that everything is clean again to run the next example.
*

  
 