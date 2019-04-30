package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
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


	private static String [] joinedNodes = new String[]{"11108","11112","11116","11120","11124"};
	private static  final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
	private static String myNode ;
	private static String myNodeHash;
	private final static int SERVER_PORT = 10000;
	private Map<String, String> nodesMap = new HashMap<String,String>();


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
			String hashedKey = genHash(fileName);
			Log.e("INSERT", "KEY TO INSERT" + fileName);
			Log.e("INSERT", "VALUE TO INSERT" + value);
			if (IsCorrectNode(hashedKey)) {
				FileOutputStream outputStream = null;
				outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
				outputStream.write(value.getBytes());
				outputStream.flush();
				outputStream.close();

			} else {


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
			//String myPortId =String.valueOf (Integer.parseInt(processId)*2);

			nodesMap.put("11108", genHash("11108"));
			nodesMap.put("11112", genHash("11112"));
			nodesMap.put("11116",  genHash("11116"));
			nodesMap.put("11120",genHash("11120"));
			nodesMap.put("11124", genHash("11124"));




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

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
           // formatter.format("%02x", b);
        }
        return formatter.toString();
    }



	private boolean IsCorrectNode(String hashedKey)
	{
		try {
			if( hashedKey.compareTo(genHash(myNode)) < 0 ){
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


}
