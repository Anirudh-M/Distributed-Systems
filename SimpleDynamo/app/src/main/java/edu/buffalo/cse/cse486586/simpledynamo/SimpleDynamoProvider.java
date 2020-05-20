package edu.buffalo.cse.cse486586.simpledynamo;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


class Message implements Serializable {
	String key;
	String value;
	String type;

	Message(String value, String type, String key){
		this.type = type;
		this.key = key;
		this.value = value;
	}
}

class Node {
	String key;
	String port;

	Node(String key, String port)
	{
		this.port = port;
		this.key = key;
	}
}



public class SimpleDynamoProvider extends ContentProvider {


	String[] ports = {"5554", "5556", "5558", "5560", "5562"};
	ArrayList<Node> nodes = new ArrayList<Node>();
	static final int SERVER_PORT = 10000, neighbours = 3;
	private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	String portString;
	ArrayList<String> hashNodes = new ArrayList<String>();


	private MatrixCursor getData(String filename, MatrixCursor matrixCursor){
		try {
			FileInputStream fileInputStream = getContext().openFileInput(filename);
			byte[] dataFile = new byte[fileInputStream.available()];
			fileInputStream.read(dataFile);
			matrixCursor.addRow(new Object[]{filename, new String(dataFile)});

		} catch (Exception e) {
			e.printStackTrace();
		}
		return  matrixCursor;
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.v("Delete", selection);
		if(selection.equals("*") || selection.equals("@")) {

			for(String fileName :this.getContext().fileList()) {
				getContext().deleteFile(fileName);
				nodes.remove(fileName);
			}
		}
		else {
			String origin;
			for (int i = 0; i < hashNodes.size(); i++) {
				if (match(i, selection)) {
					origin = hashNodes.get(i);
					File file = new File(getContext().getFilesDir(), selection);
					file.delete();
					if(!origin.equals(portString)) {
						Message msg = new Message(selection, "DREQ", origin);
						handleClient(msg);
					}
					break;
				}
			}
		}
		return 0;
	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}


	public void insertHandler(String key, String value) {

		try{
			FileOutputStream outputStream;
			outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			outputStream.write(value.getBytes());
			outputStream.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

// Checks the location while inserting, querying and deleting

	private boolean match(int i, String values){
		try {
			return ((genHash(hashNodes.get((i + hashNodes.size() -1) % hashNodes.size())).compareTo(genHash(values)) < 0
					&& genHash(hashNodes.get(i % hashNodes.size())).compareTo(genHash(values)) > -1) ||
					((genHash(hashNodes.get((i + hashNodes.size() -1)%hashNodes.size())).compareTo(genHash(values)) < 0 ||
							genHash(hashNodes.get(i % hashNodes.size())).compareTo(genHash(values)) > -1) &&
							genHash(hashNodes.get((i + hashNodes.size() -1) % hashNodes.size())).compareTo(genHash(hashNodes.get(i))) > 0 ));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return  false;

	}


	@Override
	public Uri insert(Uri uri, ContentValues values) {

		try {
			for (int i = 0; i < hashNodes.size(); i++) {
				if (match(i, values.getAsString("key"))) {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT",values.getAsString("key"),values.getAsString("value"), hashNodes.get(i));
					if (hashNodes.get(i).equals(portString)) {
						insertHandler(values.getAsString("key"), values.getAsString("value"));
						nodes.add(new Node(values.getAsString("key"),portString));
					}
					break;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return mUri;
	}


	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portString = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INITIATE", portString);
		}
		catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

// Handles indexes of nodes in hashnodes on initiation

	public void handleNodes(){
		for (String port: ports) {
			int count = 0;
			for (String node: hashNodes) {
				try {
					if (genHash(node).compareTo(genHash(port)) >= 1) {
						hashNodes.add(count++,port);
						break;
					}

				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
			if(!hashNodes.contains(port)) {
				hashNodes.add(port);
			}

		}
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {

		MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});

		try{
			if(selection.equals("@") || selection.equals("*")) {

				for(String fileName :getContext().fileList()) {
					Log.v("QUERY", fileName);
					matrixCursor = getData(fileName, matrixCursor);
				}
				if(selection.equals("*")) {
					for(int j=0; j<hashNodes.size(); j++) {
						if(!portString.equals(hashNodes.get(j))) {
							Message msg = new Message(selection, "QREQ", hashNodes.get(j));
							String response = handleClient(msg);
							String[] temp = response.split(":");
							for(int i=0; i<temp.length-1; i+=2) {
								matrixCursor.addRow(new Object[]{temp[i+1], temp[i]});
							}

						}
					}
				}

				return matrixCursor;
			}

			String origin = null;
			for(int j = 0;j < hashNodes.size(); j++){
				if(match(j, selection)){
					origin = hashNodes.get(j);
					if(hashNodes.get(j).equals(portString)) { break;}
				}
			}
			matrixCursor = getData(selection, matrixCursor);
			matrixCursor = cursorHandler(origin, matrixCursor, selection);

		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return matrixCursor;
	}

	public MatrixCursor cursorHandler(String portFrom, MatrixCursor matrixCursor, String selection){

		int i =0;
		String response = null;
		while(response == null || response.length()==0) {
			Message msg = new Message(selection, "QREQ", hashNodes.get((hashNodes.indexOf(portFrom)+(i++)%neighbours)%hashNodes.size()));
			response = handleClient(msg);
		}
		matrixCursor.addRow(new Object[]{selection, response.split(":")[0]});
		return matrixCursor;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	public Uri buildUri(String scheme, String authority) {

		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			while (true) {
				try {
					Socket socket = serverSocket.accept();
					socket.setSoTimeout(3000);

					ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
					Message m = (Message) inputStream.readObject();
					String message = m.value;

					Log.v("Serverside ",message);
					conditions c = conditions.valueOf(m.type);
					Message msg;
					MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key","value"});

					switch (c){

						case INSERT:

							String[] temp = message.split(":");
							insertHandler(temp[0], temp[1]);
							nodes.add(new Node(temp[0],temp[2]));
							ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
							outputStream.close();
							break;

						case INITIATE:
							for(Node insertValue: nodes) {
								if (insertValue.port.equals(message)) {
									matrixCursor = getData(insertValue.key, matrixCursor);
								}
							}
							String returnMatrix = matrixToString(matrixCursor);
							outputStream = new ObjectOutputStream(socket.getOutputStream());
							msg = new Message(returnMatrix, "ACKR", null);
							outputStream.writeObject(msg);
							outputStream.flush();
							outputStream.close();
							break;

						case QUERY:
							if(message.equals("*")) {
								for(Node insertValue: nodes) {
									matrixCursor = getData(insertValue.key, matrixCursor);
								}
							}
							else {
								matrixCursor = getData(message, matrixCursor);
							}
							returnMatrix = matrixToString(matrixCursor);
							outputStream = new ObjectOutputStream(socket.getOutputStream());

							msg = new Message(returnMatrix, "ACKQ", null);
							outputStream.writeObject(msg);
							outputStream.flush();
							outputStream.close();
							matrixCursor.close();
							break;

						case DELETE:
							temp = message.split(":");
							if(temp[0].equals("*")) {
								delete(mUri, temp[0]+":"+temp[1], null);
							}
							else {
								delete(mUri, temp[0], null);
							}
							outputStream = new ObjectOutputStream(socket.getOutputStream());
							msg = new Message("ACK", "ACKD", null);

							outputStream.writeObject(msg);
							outputStream.flush();
							outputStream.close();
							break;
					}

					inputStream.close();
				}
				catch (Exception e) {
					e.printStackTrace();
				}

			}
		}

		protected void onProgressUpdate(String... msg) {
			query(mUri, null, msg[0].trim(), null, null);
		}

		// Converting matrix to string to pass in Message

		private String matrixToString(MatrixCursor matrixCursor){
			StringBuilder retrieveAck = new StringBuilder();
			while(matrixCursor.moveToNext()) {
				String valueData = matrixCursor.getString(matrixCursor.getColumnIndex("value"));
				String keyData = matrixCursor.getString(matrixCursor.getColumnIndex("key"));
				retrieveAck.append(valueData+":"+keyData+":");
			}
			matrixCursor.close();
			return  retrieveAck.toString();
		}
	}

	private enum conditions{
		INSERT, QUERY, DELETE, INITIATE
	}



	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			ObjectOutputStream outputStream;
			String message = msgs[0];
			Log.v("Clientside", message);
			if(message.equals("INITIATE")) {
				handleNodes();
				String m1, m2;
				for(int j = 0; j > - neighbours; j--){
					m1 = hashNodes.get((hashNodes.indexOf(msgs[1]) + hashNodes.size() + j)%hashNodes.size());
					if(j == 0){
						m2 = hashNodes.get((hashNodes.indexOf(msgs[1]) + hashNodes.size()+1)%hashNodes.size());
					}else {
						m2 = hashNodes.get((hashNodes.indexOf(msgs[1]) + hashNodes.size() + j) % hashNodes.size());
					}

					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(m2)*2);

						outputStream = new ObjectOutputStream(socket.getOutputStream());
						Message msg = new Message(m1, "INITIATE", null);
						outputStream.writeObject(msg);
						outputStream.flush();
						socket.setSoTimeout(3000);

						ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
						Message ack = (Message) inputStream.readObject();
						String[] response = ack.value.split(":");

						for(int i=0; i<response.length-1; i+=2) {
							insertHandler(response[i+1], response[i]);
							nodes.add(new Node(response[i+1], m1));
						}

						inputStream.close();
						outputStream.close();
						socket.close();
					}catch (Exception e) {
						e.printStackTrace();
					}

				}
			}

			else{
				String msgToSend =msgs[1]+":"+msgs[2]+":"+msgs[3];
				for(int i=0; i<neighbours; i++) {
					try
					{
						String portSend = hashNodes.get(((hashNodes.indexOf(msgs[3]))+i)%(hashNodes.size()));
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(portSend)*2);

						socket.setSoTimeout(3000);
						outputStream = new ObjectOutputStream(socket.getOutputStream());
						Message msg = new Message(msgToSend, "INSERT", null);
						outputStream.writeObject(msg);
						outputStream.flush();
						ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
						outputStream.close();
						socket.close();
					}catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			return null;
		}
	}


	// Handling query and delete requests

	public String handleClient(Message m)
	{
		String msgToSend = null;
		String type = null;

		if(m.type == "QREQ"){
			msgToSend = m.value;
			type = "QUERY";
		}else if(m.type == "DREQ"){
			if(m.value.equals("*")) {
				msgToSend = m.value+":"+portString;
			}
			else {
				msgToSend = m.value;
			}
			type = "DELETE";
		}

		ObjectOutputStream outputStream = null;
		String response = "";

		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(m.key)*2);
			socket.setSoTimeout(3000);

			outputStream = new ObjectOutputStream(socket.getOutputStream());
			Message msg = new Message(msgToSend, type, null);
			outputStream.writeObject(msg);
			outputStream.flush();

			Log.v("handleClient:", "Connecting - "+m.key);

			ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
			Message r = (Message) inputStream.readObject();

			inputStream.close();
			outputStream.close();
			socket.close();
			return r.value;

		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return response;

	}


	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


}