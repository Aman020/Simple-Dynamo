package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;



public class SimpleDynamoProvider extends ContentProvider {

	Helper helper = new Helper();
	//private static String [] joinedNodes = new String[]{"11108","11112","11116","11120","11124"};
	private static  final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");

	private static String myPort ;
	private static String myHashedPort;
	private final static int SERVER_PORT = 10000;
	private static List<Node> joinedNodes;
	private static MatrixCursor singleKeyResult;
	private static MatrixCursor starKeyResult;
	private static int startQueryCount =1;
    private static HashMap<String, MatrixCursor> cursorLocks = new HashMap<String, MatrixCursor>();
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        try {

            File filesDirectory = getContext().getFilesDir();
            File[] files = filesDirectory.listFiles();

           // if (selection.equals("@")|| selection.equals("*")) {
                Log.e("No of files", "" + files.length);
                for (File file : files) {
                    file.delete();

                }

//            }// The below code is a part in plagiarism. Please do not copy.
//            else{
//                boolean isFileAvailable = false;
//                for (File file : files){
//                    if(file.getName().equals(selection))
//                    {
//                        isFileAvailable = true;
//                        file.delete();
//                        Log.e("In delete at "+ myPort,"****FILE DELETED****");
//                        break;
//                    }
//                }
//                if( !isFileAvailable)
//                {
//
//                }
//            }



        }catch(Exception ex)
        {
            ex.printStackTrace();
        }

        // TODO Auto-generated method stub
        return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		ServerSocket serverSocket  = null;
		try
		{
			serverSocket = new ServerSocket(SERVER_PORT);
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			final String processId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			myHashedPort = helper.genHash(processId);
			myPort =String.valueOf (Integer.parseInt(processId)*2);
			joinedNodes = new ArrayList<Node>();
			joinedNodes.add(new Node("11108"));
			joinedNodes.add(new Node("11112"));
			joinedNodes.add(new Node("11116"));
			joinedNodes.add(new Node("11120"));
			joinedNodes.add(new Node("11124"));
			Collections.sort(joinedNodes, new NodeCompare());
			Log.e("On create", "Sorted list");
			StringBuilder sb = new StringBuilder();

			for(Node node: joinedNodes)
			{
				sb.append(node.portId +"--->");

			}
			Log.e("Sorted List", sb.toString());
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.e("*********","****MY PORT ID*****----"+ Integer.parseInt(processId)*2);

		}
		catch(Exception ex)
		{
			Log.e("On Create", " Something went wrong in on create function");
			ex.printStackTrace();
		}
		return false;
	}

	private class ClientTask extends  AsyncTask<String, Void, Void>{

		@Override
		protected Void doInBackground(String... strings) {
			try {
				String message = strings[0];
				String []messageTokens = strings[0].split(":");
				String toSendPort= messageTokens[3];
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(toSendPort));
				DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
				outputStream.writeUTF(message);
				outputStream.flush();
			}catch(Exception ex)
			{
				Log.e("In client", " Something went wrong in client Async Task");
				ex.printStackTrace();
			}
			return null;
		}
	}
	private class ServerTask extends AsyncTask<ServerSocket, String, Void>
	{
		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			try
			{

				while (true) {

					Socket socket = serverSockets[0].accept();
					DataInputStream inputStream = new DataInputStream(socket.getInputStream());
					String messageFromClient = inputStream.readUTF();
					String[] messageFromClientTokens = messageFromClient.split(":");
					String operation = messageFromClientTokens[0];

					if( operation.equals("Insert")){
						Log.i("SERVER-" + myPort,"Replication of key received-"+messageFromClientTokens[1] );
						String key = messageFromClientTokens[1];
						String value = messageFromClientTokens[2];
						Insert(key,value);
					}
					else if (operation.equals("Check Key")){
                        String whoWantsKey = messageFromClientTokens[2];
                        Log.e("Inside server- " + myPort, "Check key requested by" + whoWantsKey );
                        ReturnSingleKey(messageFromClientTokens[1], whoWantsKey);

                    }
                    else if (operation.equals("Found Key")){
                        MatrixCursor cursor = cursorLocks.get(messageFromClientTokens[1]);
                        cursor.addRow( new String[]{messageFromClientTokens[1],messageFromClientTokens[2]});

                        synchronized (cursor)
                        {
                            cursor.notify();
                        }
                        Log.e("Inside server -"+ myPort, "Key Received-" +messageFromClientTokens[1] );
                    }
                    else if (operation.equals("Retrieve")){
                        String starQueryResquestedPort = messageFromClientTokens[1];
                        SendMyQueryResult(starQueryResquestedPort);


                    }
                    else if(operation.equals("Retrieve Result")){
                        startQueryCount ++;
                        Log.e("At server-"+ myPort, "Key received from" + messageFromClientTokens[2]);
                        String allKeyValuePairs = messageFromClientTokens[1];
                        String[] allKeyValuePairsTokens = allKeyValuePairs.split("@@");

                        int count = 0;
                        for (String keyValue : allKeyValuePairsTokens) {
                            String[] keyAndValue = keyValue.split("-");
                            starKeyResult.addRow(new String[]{keyAndValue[0], keyAndValue[1]});
                            count++;
                        }
                        Log.e("No of keys retreived", String.valueOf(count));

                        if( startQueryCount == 4)
                        {
                            Log.e("At server-" + myPort, "Retrieved star result from all the ports");
                            synchronized (starKeyResult) {
                                starKeyResult.notify(); }
                        }

                        }
				}
			}
			catch(Exception ex)
			{
				Log.e("In server", " Something went wrong in the server code");
				ex.printStackTrace();
			}
			return null;
		}
	}

	private void SendMyQueryResult(String starQueryResquestedPort)
    {
        Log.e("At port-" + myPort, "Retrieving my keys");
        Cursor cursor = getContext().getContentResolver().query(CONTENT_URI, null, "@", null, null);
        StringBuilder sb = new StringBuilder();
        sb.append(KeyValuePairString(cursor));
        Log.e("My keys", sb.toString());
        Log.e("Sending my keys to -",starQueryResquestedPort);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Retrieve Result:" + sb.toString() +":" + myPort + ":" + starQueryResquestedPort);

    }
    private void ReturnSingleKey(String selection, String whoWantsKey)
    {
        try {
                FileInputStream inputstream = getContext().openFileInput(selection);
                int res = 0;
                // creating an object of StringBuilder to efficiently append the data which is read using read() function. Read() function returns a byte and hence we use while loop to read all the bytes. It returns -1 if its empty.
                StringBuilder sb = new StringBuilder();
                while ((res = inputstream.read()) != -1) {
                    sb.append((char) res);
                }
                singleKeyResult = new MatrixCursor(new String[]{"key","value"});
                singleKeyResult.addRow(new String[]{selection, sb.toString()});
                Log.e("At port-" + myPort,"Found key -" + selection +"-" + sb.toString());
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Found Key:" + selection +":" + sb.toString() +":" + whoWantsKey + ":" + myPort);
                inputstream.close();
        }catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            Log.e("In Insert", "Calling insert");
            String fileName = values.getAsString("key");
            String value = values.getAsString("value");
            String hashedKey = helper.genHash(fileName);
            Log.e("INSERT", "KEY TO INSERT " + fileName);
            List<String> keySendToPort = findCorrectAndReplicatedPort(hashedKey);
            Log.e("Insert key in ", keySendToPort.get(0) +"-" +  keySendToPort.get(1) + keySendToPort.get(2));
                for(String port: keySendToPort){
                    Log.e("Replicating key at" + port, fileName);
                    if(port.equals(myPort))
                    {
                        Log.e("Inserting key at " +myPort ,fileName);
                        Insert(fileName,value);
                    }
                    else
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Insert:"+ fileName + ":" +value + ":" + port);
                }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
            Log.e("In insert","Something went wrong");
        }
        // TODO Auto-generated method stub
        return null;
    }




	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		Log.e("Inside Query---","Request for key-" + selection);
		// TODO Auto-generated method stub
		MatrixCursor cursor;
		try {
			if (selection.equals("@")) {
				return GetLocalKeys();
			}
			else if (selection.equals("*")) {
			    Log.e("At port-" + myPort,"* Query request received.Requesting everyone to send their keys ");
			   return RetrieveAllKeys();
			}
			else
            {
				try {
				    Log.e("Query for key -", selection );
                    String path=getContext().getFilesDir().getAbsolutePath()+"/" +selection;
                    File file = new File ( path );
                    if(file.exists())
                    {
                        Log.e("At port-"+ myPort,"I have the file");
                        FileInputStream inputstream = getContext().openFileInput(selection);
                        int res = 0;
                        // creating an object of StringBuilder to efficiently append the data which is read using read() function. Read() function returns a byte and hence we use while loop to read all the bytes. It returns -1 if its empty.
                        StringBuilder sb = new StringBuilder();
                        while ((res = inputstream.read()) != -1) {
                            sb.append((char) res);
                        }
                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                        matrixCursor.addRow(new String[]{selection, sb.toString()});
                        Log.e("In query","Returning the key - "+ selection);
                        inputstream.close();
                        return  matrixCursor;

                    }
                    else {
                        Log.e("Single key query", " I dont have the key. Asking other nodes");
                        MatrixCursor singleKeyResult = new MatrixCursor(new String[]{"key", "value"});
                        cursorLocks.put(selection,singleKeyResult);
                        List<String> actualKeyPorts = findCorrectAndReplicatedPort(helper.genHash(selection));
                        Log.e("Key should be in ports",actualKeyPorts.get(0)+ "-" + actualKeyPorts.get(1) + "-" + actualKeyPorts.get(2));
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Check Key:" + selection + ":" + myPort + ":" + actualKeyPorts.get(0));
                        Log.e("In query else", " Running the lock now");
                        try {
                            synchronized (singleKeyResult) {
                                singleKeyResult.wait();
                            }
                            Log.e("In query", "Thread has been notified to run");
                            return singleKeyResult;
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
				}catch (Exception ex)
				{
					ex.printStackTrace();
				}
			}

		}catch(Exception ex)
		{
			Log.e("In query","Someting went wrong");
			ex.printStackTrace();
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private MatrixCursor GetLocalKeys() {
		Log.e("Inside query"," @ running");
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		File filesDirectory = getContext().getFilesDir();
		File[] files = filesDirectory.listFiles();
		Log.e("No of files","" +files.length );
		try {
			for (File file : files) {
				String key = file.getName();
				// Creating a stream object. Various other alternate byte/character streams can be used to read and write the data.
				BufferedReader inputstream = new BufferedReader(new InputStreamReader(getContext().openFileInput(key)));
				String value = inputstream.readLine();
				Log.e("Key value-", key + "---" + value);
				cursor.addRow(new String[]{key, value});
			}
			return cursor;
		}catch (Exception ex)
		{
			Log.e("In GetLocalKeys", "Something went wrong");
		}
		return null;
	}
    private MatrixCursor RetrieveAllKeys()
    {
        try
        {
            starKeyResult = new MatrixCursor(new String [] {"key","value"});
            starKeyResult = GetLocalKeys();
            for(Node node :joinedNodes) {
                if( node.portId.equals(myPort))
                {
                    continue;
                }
                else {
                    Log.e("RetrieveAllKeys -" , node.portId);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Retrieve:" + myPort + ":" + "*" + ":" + node.portId);
                }
            }

            synchronized(starKeyResult)
            { starKeyResult.wait();}
            Log.e("In query", "Thread has been notified to run");
            return starKeyResult;
        }catch (Exception ex)
        {
            Log.e("In RetrieveKeys","Something went wrong");
            ex.printStackTrace();
        }return null;
    }

    private String  KeyValuePairString(Cursor cursor)
    {
        StringBuilder sb = new StringBuilder();
        while ((cursor.moveToNext()))
        {
            String key = cursor.getString(cursor.getColumnIndex("key"));
            String value = cursor.getString(cursor.getColumnIndex("value"));
            sb.append(key +"-" + value +"@@");
        }
        return sb.toString();

    }

    private boolean IsCorrectNode(String hashedKey) {
		try {
			if( hashedKey.compareTo(myHashedPort) < 0 ){
				return true;
			}
			return false;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();

		}
		return false;
	}
	private List<String> findCorrectAndReplicatedPort(String hashedKey) {
		int index =-1;
		for (Node node :joinedNodes)
		{
			Log.e("Inside for loop", node.portId);
			if( hashedKey.compareTo(node.hashedId) <0) {
				index = joinedNodes.indexOf(node);
				Log.e("Index value",index + "");
				break;
			}
		}
		if (index == -1 ) return Arrays.asList(joinedNodes.get(0).portId,joinedNodes.get(1).portId, joinedNodes.get(2).portId);
		if( index == joinedNodes.size() -1)
			return Arrays.asList(joinedNodes.get(index).portId,joinedNodes.get(0).portId, joinedNodes.get(1).portId);
		else if(index == joinedNodes.size() -2)
			return Arrays.asList(joinedNodes.get(index).portId,joinedNodes.get(index +1).portId, joinedNodes.get(0).portId);
		else
			return Arrays.asList(joinedNodes.get(index).portId,joinedNodes.get(index + 1).portId, joinedNodes.get(index +2).portId);
	}
	private void Insert(String key, String value){
			try{
				FileOutputStream outputStream = null;
				outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.flush();
				outputStream.close();
			}
			catch (Exception ex)
			{
				ex.printStackTrace();
				Log.e("In Insert function", " Error in insert function");
			}

	}

}
