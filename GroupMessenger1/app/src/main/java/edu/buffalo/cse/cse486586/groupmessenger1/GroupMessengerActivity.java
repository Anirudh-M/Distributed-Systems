package edu.buffalo.cse.cse486586.groupmessenger1;

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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    int count = 0;
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    String[] ports = {"11108", "11112", "11116", "11120", "11124"};



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
        final EditText editText = (EditText) findViewById(R.id.editText1);


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1
        );
        tv.setMovementMethod(new ScrollingMovementMethod());

        Button send = (Button) findViewById(R.id.button4);
        //send.setOnClickListener(this);


        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }


        send.setOnClickListener(
                new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {

                        String msg = editText.getText().toString() + "\n";
                        editText.setText("");

                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, myPort);

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


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

                try {
                    while (true) {


                        Socket socket = serverSocket.accept();
                        //Log.v("Socket", socket.toString());
                        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String message = br.readLine();
                        //message = message.trim();
                        //Log.v(TAG, "The message is " + message);


                        if (message != null) {
                            publishProgress(message);
                        }
                        //br.close();
                        //socket.close();
                    }

                } catch (Exception e) {
                    Log.e(TAG, "No messages");
                }

            return null;
        }

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }
        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String msg = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(msg + "\t\n");

            final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
            ContentValues keyVal = new ContentValues();


            keyVal.put("key", String.valueOf(count));
            keyVal.put("value", msg);

            Log.d("Filename - value",(count)+ " - " + msg);
            count++;



            Uri newUri = getContentResolver().insert(
                    mUri,    // assume we already created a Uri object with our provider URI
                    keyVal
            );


            return;
        }
    }



    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                String msgToSend = msgs[0];
                Socket[] sockets = new Socket[5];

                for(int i = 0; i<5 ; i++) {

                    sockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(ports[i]));



                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(sockets[i].getOutputStream()));
                    bw.write(msgToSend);

                    Log.v(TAG, "Client message " + msgToSend);
                    Log.v("Port number", msgs[1]);

                    bw.flush();
                    //socket.close();
                }

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */


            }
            catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;

        }
    }


}