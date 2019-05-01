package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;



public class SimpleDynamoProvider extends ContentProvider {

	Helper helper = new Helper();
	//private static String [] joinedNodes = new String[]{"11108","11112","11116","11120","11124"};
	private static  final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
	private static String myNode ;
	private static String myNodeHash;
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
	public Uri insert(Uri uri, ContentValues values) {
		try {
			Log.e("In Insert", "Calling insert");
			String fileName = values.getAsString("key");
			String value = values.getAsString("value");
			String hashedKey = helper.genHash(fileName);
			Log.e("INSERT", "KEY TO INSERT" + fileName);
			Log.e("INSERT", "VALUE TO INSERT" + value);
			if (IsCorrectNode(hashedKey)) {
				FileOutputStream outputStream = null;
				outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.flush();
				outputStream.close();

			} else {
					String sendKeyToPort = findCorrectPort(hashedKey);

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
	public boolean onCreate() {
		// TODO Auto-generated method stub
		ServerSocket serverSocket  = null;
		try
		{

			serverSocket = new ServerSocket(SERVER_PORT);
			TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
			final String processId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
			String myPortId =String.valueOf (Integer.parseInt(processId)*2);
			joinedNodes.add(new Node("11108"));
			joinedNodes.add(new Node("11112"));
			joinedNodes.add(new Node("11116"));
			joinedNodes.add(new Node("11120"));
			joinedNodes.add(new Node("11124"));
			Collections.sort(joinedNodes, new NodeCompare());
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
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}




	private boolean IsCorrectNode(String hashedKey)
	{
		try {
			if( hashedKey.compareTo(helper.genHash(myNode)) < 0 ){
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


	private String findCorrectPort(String hashedKey)
	{
		return null;

	}




}
