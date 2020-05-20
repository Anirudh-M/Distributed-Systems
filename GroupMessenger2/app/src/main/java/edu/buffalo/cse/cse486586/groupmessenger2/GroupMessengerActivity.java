package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import java.net.UnknownHostException;
import java.util.Arrays;


/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */


class Message implements Serializable{

    String message;
    float priority;


    public void initialize(String message, float priority){
        this.message = message;
        this.priority = priority;
    }

    public String getMessage() {
        return message;
    }

    public float getPriority() {
        return priority;
    }

}


public class GroupMessengerActivity extends Activity {
    static final String REMOTE_PORT[] = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000, numPhones = 5;
    static int agreed = 0, proposed = 0, avd_id = 0;
    boolean failure[] = new boolean[numPhones];
    private Uri mUri;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final int port =  Integer.parseInt(portStr) * 2;
        Arrays.fill(failure, false);

        for(int i = 0; i < REMOTE_PORT.length; i ++){
            if(port == Integer.parseInt(REMOTE_PORT[i])){
                avd_id = i+1;
                break;
            }
        }


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button button4 = (Button) findViewById(R.id.button4);

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        button4.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString();
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }



    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }


        private void handleContentResolver(ContentValues mContentValues,  Message message, float count){
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
            mContentValues.put("key", count);
            mContentValues.put("value", message.getMessage());
            Log.v("Key - value", String.valueOf(count) + " - " + message.getMessage());
            getContentResolver().insert(mUri, mContentValues);
            publishProgress(message.getMessage());
            mContentValues.clear();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            float msg_pri;
            ServerSocket serverSocket = sockets[0];
            ContentValues mContentValues = new ContentValues();

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */



            try {
                while (true) {
                    try {
                        Socket server = serverSocket.accept();

                        ObjectOutputStream outstream = new ObjectOutputStream(server.getOutputStream());
                        ObjectInputStream instream = new ObjectInputStream((server.getInputStream()));

                        Message msg = (Message) instream.readObject();

                        msg_pri = msg.getPriority();
                        if (msg_pri == -1) {
                            proposed = Math.max(proposed, agreed) + 1;
                            String proposed_seq_avd = proposed + "." + avd_id;

                            Message proposed = new Message();
                            proposed.initialize("Proposed", Float.parseFloat(proposed_seq_avd));

                            outstream.writeObject(proposed);
                            outstream.flush();
                            outstream.close();
                            instream.close();
                            server.close();
                        } else {

                            agreed = Math.max((int) msg_pri, agreed);

                            Message end = new Message();
                            end.initialize("Message sent", -5);
                            outstream.writeObject(end);
                            outstream.flush();
                            handleContentResolver(mContentValues, msg, msg_pri);

                            outstream.close();
                            instream.close();
                            server.close();

                        }
                    }

                    catch (StreamCorruptedException e) {
                        e.printStackTrace();
                    } catch (EOFException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\n");

            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket[] socket = new Socket[numPhones];
            ObjectInputStream[] ins = new ObjectInputStream[numPhones];
            ObjectOutputStream[] outs = new ObjectOutputStream[numPhones];
            float seqNum = 0;
            Message message = new Message();

            for(int i = 0; i < numPhones; i++) {
                try {
                    socket[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT[i]));

                    outs[i] = new ObjectOutputStream(socket[i].getOutputStream());
                    ins[i] = new ObjectInputStream(socket[i].getInputStream());

                } catch (UnknownHostException e) {
                    failure[i] = true;
                    e.printStackTrace();
                } catch (IOException e) {
                    failure[i] = true;
                    e.printStackTrace();

                }
            }

            for (int i = 0; i < numPhones; i++) {
                if(!failure[i]) {
                    try {
                        message.initialize(msgs[0], -1);
                        outs[i].writeObject(message);
                        outs[i].flush();
                        Message received = (Message) ins[i].readObject();

                        float recvd_p = received.getPriority();
                        if (recvd_p > seqNum) {
                            seqNum = recvd_p;
                        }

                        ins[i].close();
                        outs[i].close();
                        socket[i].close();

                    } catch (StreamCorruptedException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    } catch (EOFException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    } catch (IOException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }



            for (int i = 0; i < numPhones; i++) {
                if(!failure[i]) {
                    try {
                        socket[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT[i]));
                        outs[i] = new ObjectOutputStream(socket[i].getOutputStream());
                        ins[i] = new ObjectInputStream(socket[i].getInputStream());
                        message.initialize(msgs[0], seqNum);
                        outs[i].writeObject(message);
                        outs[i].flush();
                        Message received = (Message) ins[i].readObject();
                        if(received.getMessage().equals("Message sent")){
                            ins[i].close();
                            outs[i].close();
                            socket[i].close();
                        }


                    } catch (StreamCorruptedException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    } catch (EOFException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    }catch (UnknownHostException e){
                        failure[i] = true;
                        e.printStackTrace();
                    }
                    catch (FileNotFoundException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    } catch (IOException e) {
                        failure[i] = true;
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }

            return null;
        }
    }
}