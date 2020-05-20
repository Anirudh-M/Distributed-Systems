package edu.buffalo.cse.cse486586.simpledht;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;



import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


class Message implements Serializable{
    String key;
    String value;
    String type;

    Message(String value, String type, String key){
        this.type = type;
        this.value = value;
        this.key = key;
    }
}


class Node{
    String nodeID;
    String predecessor;
    String successor;

    Node(String nodeID,String predecessor,String successor){
        this.nodeID = nodeID;
        this.predecessor = predecessor;
        this.successor = successor;
    }
}


public class SimpleDhtProvider extends ContentProvider {

    static Node node;
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    static final int SERVER_PORT = 10000;
    static String portString;
    boolean first = true;
    static ArrayList<String> filesList = new ArrayList<String>();


    public String hashing(String text) {
        try {
            return genHash(text);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    private String nodePosition(String id){

        Node n = node;
        if(first) {
            first = false;
            return node.nodeID + ":" + node.nodeID;
        }
        try {
            while(!check(genHash(id), n)){
                Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(n.successor)*2);
                ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());

                ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                Message msg = new Message("POSITION", "POSITION", null);
                outputStream.writeObject(msg);
                outputStream.flush();
                Message ack = (Message) inputStream.readObject();

                String[] vals = ack.value.split(":");
                n = new Node(vals[0], vals[1], vals[2]);
                client.close();
                outputStream.close();
                inputStream.close();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return n.nodeID + ":" + n.successor;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try {
            if(match(genHash(portString))){
                getContext().deleteFile(selection);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    private boolean check(String id, Node n){

        BigInteger idVal = new BigInteger(id, 16);
        BigInteger nodeVal = new BigInteger(hashing(n.nodeID), 16);
        BigInteger successorVal = new BigInteger(hashing(n.successor), 16);
        return(idVal.compareTo(nodeVal) > 0 && nodeVal.compareTo(successorVal) > 0 && idVal.compareTo(successorVal) > 0
                || (idVal.compareTo(nodeVal) > 0 && idVal.compareTo(successorVal) <= 0)
                || (idVal.compareTo(nodeVal) < 0 && nodeVal.compareTo(successorVal) > 0 && idVal.compareTo(successorVal) < 0));
    }


    private MatrixCursor queryHandler(String selection) throws Exception {

        MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key", "value"});
        Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(node.successor) * 2);
        Message msg = new Message(selection, "QUERY", null);
        ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
        outputStream.writeObject(msg);
        outputStream.flush();
        Message ack = (Message) inputStream.readObject();
        client.close();
        return generateMatrix(ack.value, matrixCursor);
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String fileName = (String) values.get("key");
        String fileContent = (String) values.get("value");
        try {
            String hashFileName = genHash(fileName);
            if(match(hashFileName)){
                FileOutputStream outputStream = getContext().openFileOutput(fileName, Context.MODE_PRIVATE);
                outputStream.write(fileContent.getBytes());
                outputStream.close();
                filesList.add(fileName);

            } else {

                Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(node.successor) * 2);

                Message msg = new Message(fileContent, "INSERT", fileName);
                ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                outputStream.writeObject(msg);
                outputStream.flush();
                Log.v("Client", "Closing - Inserted");
                client.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        Log.v("insert", values.toString());
        return uri;
    }


    private MatrixCursor retrieve(ArrayList<String> filesToRead){
        FileInputStream inputStream;
        MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key", "value"});
        for(String fileName : filesToRead){
            try {
                inputStream = getContext().openFileInput(fileName);
                byte[] values = new byte[inputStream.available()];
                inputStream.read(values);
                matrixCursor.addRow(new Object[]{fileName, new String(values)});
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return matrixCursor;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixCursor matrixCursor = retrieve(filesList);
        String mcString = "";
        String sel;
        try {
            if(!selection.contains(":")){
                sel = selection;
                selection = portString + ":" + selection;
            }else{
                sel = selection.split(":")[1];
            }
            if(!selection.contains(node.successor) && sel.equals("*")) {
                Log.e("Query selection ", sel);
                try{
                    matrixCursor.moveToFirst();
                    while (!matrixCursor.isAfterLast()) {
                        String returnKey = matrixCursor.getString(matrixCursor.getColumnIndex("key"));
                        String returnValue = matrixCursor.getString(matrixCursor.getColumnIndex("value"));
                        mcString += returnKey + ":" + returnValue + ":";
                        matrixCursor.moveToNext();
                    }
                    mcString = mcString.substring(0, mcString.length() - 1);
                } catch(Exception e){
                     e.printStackTrace();
                }
                return generateMatrix(mcString, queryHandler(selection));
            }

            if(!sel.equals("*") && !sel.equals("@")){
                ArrayList<String> files = new ArrayList<String>();
                String hashFileName = genHash(sel);
                if(match(hashFileName)){
                    files.add(sel);
                    return retrieve(files);
                }
                return queryHandler(selection);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        Log.v("query", selection);
        return matrixCursor;
    }


    private boolean match(String filename){
        return ((hashing(node.nodeID).compareTo(hashing(node.predecessor)) > 0 && filename.compareTo(hashing(node.predecessor)) > 0
                && filename.compareTo(hashing(node.nodeID)) <= 0) || (hashing(node.nodeID).compareTo(hashing(node.predecessor)) < 0
                && ((filename.compareTo(hashing(node.nodeID)) > 0 && filename.compareTo(hashing(node.predecessor)) > 0)
                || (filename.compareTo(hashing(node.predecessor)) < 0 && filename.compareTo(hashing(node.nodeID)) <= 0)))
                || (hashing(node.nodeID).compareTo(hashing(node.successor)) == 0 && hashing(node.nodeID).compareTo(hashing(node.predecessor)) == 0));
    }


    private MatrixCursor generateMatrix(String value, MatrixCursor matrixCursor){
        if(value.length() > 0) {
            String[] keyValPairs = value.split(":");
            int i = 0;
            while (i < keyValPairs.length) {
                matrixCursor.addRow(new Object[]{keyValPairs[i], keyValPairs[i+1]});
                i += 2;
            }
        }
        return matrixCursor;
    }


    @Override
    public boolean onCreate() {

        TelephonyManager telManager = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portString = telManager.getLine1Number().substring(telManager.getLine1Number().length() - 4);
        node = new Node(portString, portString, portString);
        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            if(!portString.equals("5554")){
                Message snd = new Message(portString, "INITIATE", "11108");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, snd);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                while(true){
                    Socket client = serverSocket.accept();

                    ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                    ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
                    Message msg = (Message) inputStream.readObject();
                    Log.i("Server", msg.type);
                    conditions c = conditions.valueOf(msg.type);
                    Message temp = new Message("", "ACK", null);
                    switch (c){
                        case INITIATE:

                            temp.value = nodePosition(msg.value);
                            outputStream.writeObject(temp);
                            outputStream.flush();
                            break;

                        case INSERT:
                            ContentValues mContentValues = new ContentValues();
                            mContentValues.put("key", msg.key);
                            mContentValues.put("value", msg.value);
                            insert(mUri, mContentValues);
                            break;

                        case POSITION:
                            temp.value = node.nodeID + ":" + node.predecessor + ":" + node.successor;
                            outputStream.writeObject(temp);
                            outputStream.flush();
                            break;

                        case PREDECESSOR:
                            node.predecessor = msg.value;
                            break;

                        case SUCCESSOR:
                            node.successor = msg.value;
                            break;

                        case QUERY:
                            MatrixCursor matrixCursor = (MatrixCursor) query(mUri, null, msg.value, null, null);
                            try {
                                matrixCursor.moveToFirst();
                                while (!matrixCursor.isAfterLast()) {
                                    String returnKey = matrixCursor.getString(matrixCursor.getColumnIndex("key"));
                                    String returnValue = matrixCursor.getString(matrixCursor.getColumnIndex("value"));
                                    temp.value += returnKey + ":" + returnValue + ":";
                                    matrixCursor.moveToNext();
                                }
                                temp.value = temp.value.substring(0, temp.value.length() - 1);
                            } catch(Exception e){
                                e.printStackTrace();
                            }
                            outputStream.writeObject(temp);
                            outputStream.flush();
                            break;
                    }
                }
            } catch (IOException e){
                e.printStackTrace();
            } catch (Exception e){
                e.printStackTrace();
            }
            return null;
        }


    }
    private enum conditions{
        INITIATE, POSITION, SUCCESSOR, PREDECESSOR, INSERT, QUERY
    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ClientTask extends AsyncTask<Message, Void, Void>{
        @Override
        protected Void doInBackground(Message... msg) {

            try {
                Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msg[0].key));
                ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                outputStream.writeObject(msg[0]);
                outputStream.flush();
                Message ack = (Message) inputStream.readObject();
                String[] contents = ack.value.split(":");
                node.predecessor = contents[0];
                node.successor = contents[1];
                client.close();
                informClients(contents[0],"SUCCESSOR");
                informClients(contents[1],"PREDECESSOR");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        private void informClients(String port,  String type){
            try {
                Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(port)*2);

                ObjectOutputStream outputStream = new ObjectOutputStream(client.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(client.getInputStream());
                Message message = new Message(portString, type, null);
                outputStream.writeObject(message);
                outputStream.flush();
                outputStream.close();
                client.close();

            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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