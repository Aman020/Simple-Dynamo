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
	private static  final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
	private static String myPort ;
	private static String myHashedPort;
	private final static int SERVER_PORT = 10000;
	private static List<Node> joinedNodes;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
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

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            Log.e("In Insert", "Calling insert");
            String fileName = values.getAsString("key");
            String value = values.getAsString("value");
            String hashedKey = helper.genHash(fileName);
            Log.e("INSERT", "KEY TO INSERT " + fileName);
            List<String> keySendToPort = findCorrectAndReplicatedPort(hashedKey);
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
		Log.e("Inside","Query-----");
		// TODO Auto-generated method stub
		MatrixCursor cursor;
		try {
			if (selection.equals("@")) {
				return GetLocalKeys();
			}
			else if (selection.equals("*")) {

			}
			else
			{
				Log.e("Inside query"," RUNNING ELSE PART---- SINGLE KEY QUERY");
				try {
					if( IsCorrectNode(helper.genHash(selection))) {
						Log.e("Inside query"," SNGLE KEY EXISTS HERE");
						FileInputStream inputstream = getContext().openFileInput(selection);
						int res = 0;
						// creating an object of StringBuilder to efficiently append the data which is read using read() function. Read() function returns a byte and hence we use while loop to read all the bytes. It returns -1 if its empty.
						StringBuilder sb = new StringBuilder();
						while ((res = inputstream.read()) != -1) {
							sb.append((char) res);
						}
						MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
						matrixCursor.addRow(new String[]{selection, sb.toString()});
						inputstream.close();
						return  matrixCursor;
					}
					else
					{


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
