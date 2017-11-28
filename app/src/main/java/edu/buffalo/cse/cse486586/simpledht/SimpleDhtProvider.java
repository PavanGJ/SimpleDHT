package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;

public class SimpleDhtProvider extends ContentProvider {

    private final String TAG = this.getClass().getName();

    protected String NODE_ID = null;
    protected String SUCCESSOR_NODE_ID = null;
    protected String PREDECESSOR_NODE_ID = null;

    protected String PORT = null;
    protected String MASTER_NODE_PORT = "5554";
    protected String SUCCESSOR_NODE_PORT = null;
    protected String PREDECESSOR_NODE_PORT = null;

    protected final String JSON_KEY_TYPE = "type";
    protected final String JSON_KEY_SOURCE_PORT = "source";
    protected final String JSON_KEY_PREDECESSOR = "predecessor";
    protected final String JSON_KEY_SUCCESSOR = "successor";
    protected final String JSON_KEY_MESSAGE = "message";
    protected final String JSON_KEY_QUERY = "query";

    protected final int JSON_TYPE_JOIN_REQUEST = 0;
    protected final int JSON_TYPE_JOIN_RESPONSE = 1;
    protected final int JSON_TYPE_CHANGE_SUCCESSOR = 2;
    protected final int JSON_TYPE_INSERT_VALUES = 3;
    protected final int JSON_TYPE_QUERY_REQUEST_ALL = 4;
    protected final int JSON_TYPE_QUERY_REQUEST_KEY = 5;
    protected final int JSON_TYPE_DELETE = 6;

    protected PriorityBlockingQueue<JSONObject> query_response
            = new PriorityBlockingQueue<JSONObject>(1, new Comparator<JSONObject>() {
        @Override
        public int compare(JSONObject lhs, JSONObject rhs) {
            return lhs.toString().compareTo(rhs.toString());
        }
    });

    protected final String[] cursorColumns = {HashTableContract.DHT.COLUMN_NAME_KEY,
                                                HashTableContract.DHT.COLUMN_NAME_VALUE};

    private Uri URI = null;
    private SQLiteDatabase sqLiteDatabase = null;

    Uri buildURI(String scheme,String authority){
        Uri.Builder uri = new Uri.Builder();
        uri.authority(authority);
        uri.scheme(scheme);
        return uri.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(uri.compareTo(this.URI)==0) {
            switch (selection.charAt(0)){
                case '@':
                    this.sqLiteDatabase.needUpgrade(this.sqLiteDatabase.getVersion());
                    break;
                case '*':
                    this.sqLiteDatabase.needUpgrade(this.sqLiteDatabase.getVersion());
                    JSONObject deleteMsg = new JSONObject();
                    try {
                        deleteMsg.put(JSON_KEY_TYPE,JSON_TYPE_DELETE);
                        deleteMsg.put(JSON_KEY_MESSAGE,selection);
                    } catch (JSONException e) {
                        Log.e(TAG,"JSONException: "+e.getMessage());
                    }
                    String[] clientMessage = {SUCCESSOR_NODE_PORT,"*"};
                    new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                                        clientMessage);
                    break;
                default:
                    String whereClause = HashTableContract.DHT.COLUMN_NAME_KEY + " = ? ";
                    String[] whereArgs = {selection};
                    this.sqLiteDatabase.delete(
                        HashTableContract.DHT.TABLE_NAME,
                        whereClause,
                        whereArgs
                    );
                    break;
            }
            return 0;
        }
        return -1;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        if(uri.compareTo(this.URI)==0) {

            try {
                String key = (String)values.get(HashTableContract.DHT.COLUMN_NAME_KEY);
                if (
                    ((NODE_ID.compareTo(PREDECESSOR_NODE_ID) > 0 &&
                        genHash(key).compareTo(NODE_ID) <= 0 &&
                            genHash(key).compareTo(PREDECESSOR_NODE_ID) > 0) ||
                    (NODE_ID.compareTo(PREDECESSOR_NODE_ID) < 0 &&
                        (genHash(key).compareTo(NODE_ID) <= 0 ||
                            genHash(key).compareTo(PREDECESSOR_NODE_ID) > 0)))||
                    (SUCCESSOR_NODE_ID==null || SUCCESSOR_NODE_ID.compareTo(NODE_ID)==0)
                ){

                    ContentValues contentValues = new ContentValues();
                    contentValues.put(HashTableContract.DHT.COLUMN_NAME_VALUE,
                            (String) values.get(HashTableContract.DHT.COLUMN_NAME_VALUE));
                    String selection = HashTableContract.DHT.COLUMN_NAME_KEY + " = ? ";
                    String selectionArgs[] =
                            {(String) values.get(HashTableContract.DHT.COLUMN_NAME_KEY)};

                    int out = this.update(uri, contentValues, selection, selectionArgs);
                    switch (out) {
                        case -1:
                            return null;
                        case 0:
                            this.sqLiteDatabase.insert(
                                    HashTableContract.DHT.TABLE_NAME,
                                    null,
                                    values
                            );
                            break;
                        default:
                            break;
                    }
                    return uri;
                }
                else {
                    JSONObject data = new JSONObject();
                    data.put(values.getAsString(HashTableContract.DHT.COLUMN_NAME_KEY),
                            values.getAsString(HashTableContract.DHT.COLUMN_NAME_VALUE));
                    JSONObject msg = new JSONObject();
                    msg.put(JSON_KEY_TYPE,JSON_TYPE_INSERT_VALUES);
                    msg.put(JSON_KEY_SOURCE_PORT,PORT);
                    msg.put(JSON_KEY_MESSAGE,data);
                    String[] clientMessage = {SUCCESSOR_NODE_PORT,
                            msg.toString()};
                    new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,clientMessage);
                }
            } catch (Exception e) {
                Log.e(TAG, "Database Value Insertion Error: " + e.getMessage());
            }

        }
        return null;
    }

    @Override
    public boolean onCreate() {
        String SERVER_PORT = "10000";
        /*
         *  Computes port number which is to be used as node id for the Chord DHT System
         */
        TelephonyManager telephonyManager =
                (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String line1Number = telephonyManager.getLine1Number();
        this.PORT = line1Number.substring(line1Number.length() - 4);
        try {
            this.NODE_ID = this.genHash(this.PORT);
            this.PREDECESSOR_NODE_ID = this.NODE_ID;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG,"NoSuchAlgorithmException: "+e.getMessage());
        }
        this.URI = this.buildURI("content","edu.buffalo.cse.cse486586.simpledht.provider");

        /*
         * Creates a database handle to perform database storage operations
         */
        this.sqLiteDatabase = new HashTable(getContext()).getWritableDatabase();
        try {
            ServerSocket serverSocket = new ServerSocket(Integer.parseInt(SERVER_PORT));
            new DHTServer().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch(IOException e){
            Log.e(TAG,"IOException: "+e.getMessage());
        }
        /*
         *  Creates a JSON serialized message.
         */
        JSONObject joinMessage = new JSONObject();
        try {
            joinMessage.put(JSON_KEY_TYPE,JSON_TYPE_JOIN_REQUEST);
            joinMessage.put(JSON_KEY_SOURCE_PORT,this.PORT);
        } catch (JSONException e) {
            Log.e(TAG,"JSONException "+e.getMessage());
        }
        String[] clientMessage = {SUCCESSOR_NODE_PORT,joinMessage.toString()};
        new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,clientMessage);

        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder){
        try {
            MatrixCursor cursor = new MatrixCursor(cursorColumns);
            JSONObject query = new JSONObject();
            query.put(JSON_KEY_QUERY,selection);
            query.put(JSON_KEY_SOURCE_PORT,PORT);
            String[] queryMessage;
            switch (selection.charAt(0)) {
                case '*':
                    query.put(JSON_KEY_TYPE, JSON_TYPE_QUERY_REQUEST_ALL);
                    queryMessage = new String[]{SUCCESSOR_NODE_PORT,query.toString()};
                    if(SUCCESSOR_NODE_PORT!=null && SUCCESSOR_NODE_PORT.compareTo(PORT)!=0) {
                        new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMessage);
                        JSONObject response = query_response.take();
                        Iterator query_iterator = response.keys();
                        while (query_iterator.hasNext()) {
                                /*
                                 * Extracts key and creates a new row in the MatrixCursor.
                                 */
                            String key = (String) query_iterator.next();
                            String val = response.getString(key);
                            cursor.addRow(new Object[]{key,val});
                        }
                    }
                case '@':
                    JSONObject local_response = query_local(selection,sortOrder);
                    JSONObject data = (JSONObject)local_response.get(JSON_KEY_MESSAGE);
                    Iterator query_local_iterator = data.keys();
                    while(query_local_iterator.hasNext()){
                            /*
                             * Extracts key and creates a new row in the MatrixCursor.
                             */
                        String key = (String)query_local_iterator.next();
                        String val = data.getString(key);
                        cursor.addRow(new Object[]{key,val});
                    }
                    return cursor;
                default:
                    /*
                     * If the queried key is in the present node, retrieve it else pass the query
                     * to the next node.
                     */
                    if(
                        (NODE_ID.compareTo(PREDECESSOR_NODE_ID)>0 &&
                            PREDECESSOR_NODE_ID.compareTo(genHash(selection))<0 &&
                                NODE_ID.compareTo(genHash(selection))>0) ||
                        (NODE_ID.compareTo(PREDECESSOR_NODE_ID)<0 &&
                            (NODE_ID.compareTo(genHash(selection))>0 ||
                                PREDECESSOR_NODE_ID.compareTo(genHash(selection))<0))||
                                SUCCESSOR_NODE_PORT == null ||
                                SUCCESSOR_NODE_PORT.compareTo(PORT)==0
                    ){
                        JSONObject res = query_local(selection,sortOrder);
                        data = res.getJSONObject(JSON_KEY_MESSAGE);
                        Iterator query_key_iterator = data.keys();
                        while(query_key_iterator.hasNext()){
                            /*
                             * Extracts key and creates a new row in the MatrixCursor.
                             */
                            String key = (String)query_key_iterator.next();
                            String val = data.getString(key);
                            cursor.addRow(new Object[]{key,val});
                        }
                        return cursor;
                    }
                    else {
                        /*
                         *  If the key is not in the current node, pass the query on to the next
                         *  node and wait for the result.
                         */
                        query.put(JSON_KEY_TYPE, JSON_TYPE_QUERY_REQUEST_KEY);
                        queryMessage = new String[]{SUCCESSOR_NODE_PORT,query.toString()};
                        new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryMessage);
                        JSONObject response = query_response.take();
                        JSONObject results = response.getJSONObject(JSON_KEY_MESSAGE);
                        Iterator query_response_iterator = results.keys();
                        while(query_response_iterator.hasNext()){
                            /*
                             * Extracts key and creates a new row in the MatrixCursor.
                             */
                            String key = (String)query_response_iterator.next();
                            String val = results.getString(key);
                            cursor.addRow(new Object[]{key,val});
                        }
                        return cursor;
                    }
            }
        } catch (JSONException e) {
            Log.e(TAG,"JSONException: "+e.getMessage());
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG,"NoSuchAlgorithmException: "+e.getMessage());
        } catch (InterruptedException e) {
            Log.e(TAG,"InterruptedException: "+e.getMessage());
        }

        return null;
    }

    public JSONObject query_local(String selection,
            String sortOrder) {

        String dBProjection[] = {
                HashTableContract.DHT.COLUMN_NAME_KEY,
                HashTableContract.DHT.COLUMN_NAME_VALUE
        };
        String selection_args[] = null;
        String selection_criteria;
        switch (selection.charAt(0)){
            case '*':
            case '@':
                selection_criteria = null;
                break;
            default:
                selection_criteria = HashTableContract.DHT.COLUMN_NAME_KEY + " = ? ";
                selection_args = new String[] {selection};
                break;
        }
        try{
            Cursor cursor = sqLiteDatabase.query(
                    HashTableContract.DHT.TABLE_NAME,
                    dBProjection,
                    selection_criteria,
                    selection_args,
                    null,
                    null,
                    sortOrder
            );
            JSONObject query_response = new JSONObject();
            query_response.put(JSON_KEY_MESSAGE,null);
            JSONObject data = new JSONObject();
            if(cursor != null){
                cursor.moveToFirst();
                while(!cursor.isAfterLast()){
                    String key = cursor.getString(
                            cursor.getColumnIndex(
                                    HashTableContract.DHT.COLUMN_NAME_KEY
                            )
                    );
                    String val = cursor.getString(
                            cursor.getColumnIndex(
                                    HashTableContract.DHT.COLUMN_NAME_VALUE
                            )
                    );
                    data.put(key,val);
                    cursor.moveToNext();

                }
                cursor.close();
                query_response.put(JSON_KEY_MESSAGE,data);
            }
            return query_response;
        }catch(Exception e){
            Log.e(TAG,"Database Query Error: "+e.getMessage());
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        if(uri.compareTo(this.URI)==0) {
            try {
                return this.sqLiteDatabase.update(
                        HashTableContract.DHT.TABLE_NAME,
                        values,
                        selection,
                        selectionArgs
                );
            } catch(Exception e){
                Log.e(TAG,"Database Value Updation Error: "+e.getMessage());
                return -1;
            }
        }
        return -1;
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

    private class DHTClient extends AsyncTask<String,Void,Void>{

        private final String TAG = this.getClass().getName();

        @Override
        protected Void doInBackground(String... msgs){
            try {
                if(msgs[0]==null)
                    msgs[0] = MASTER_NODE_PORT;
                Socket socket = new Socket(
                        InetAddress.getByAddress(new byte[]{10,0,2,2}),
                        Integer.parseInt(msgs[0]) * 2
                );
                OutputStream outputStream = socket.getOutputStream();
                String msgLength = Integer.toString(msgs[1].getBytes().length);
                outputStream.write(msgLength.getBytes().length);
                outputStream.write(msgLength.getBytes());
                outputStream.write(msgs[1].getBytes());
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG,"UnknownHostException: "+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG,"IOException: "+e.getMessage());
            }
            return null;
        }

    }
    private class DHTServer extends AsyncTask<ServerSocket,String,Void>{

        private final String TAG = this.getClass().getName();

        @Override
        protected Void doInBackground(ServerSocket... serverSockets){
            while(true) {
                try {
                    Socket socket = serverSockets[0].accept();
                    InputStream inputStream = socket.getInputStream();
                    int length = inputStream.read();
                    byte[] msgLength = new byte[length];
                    int output = inputStream.read(msgLength);
                    if(output == -1){
                        throw new IOException("End of File Reached");
                    }
                    byte[] msg = new byte[Integer.parseInt(new String(msgLength))];
                    int out = inputStream.read(msg);
                    if (out == -1)
                        throw new IOException("End of File Reached");
                    publishProgress(new String(msg));
                    socket.close();
                } catch (IOException e) {
                    Log.e(TAG, "IOException: " + e.getMessage());
                }
            }

        }

        @Override
        protected void onProgressUpdate(String... strings){
            try {
                JSONObject msg = new JSONObject(strings[0]);
                switch (msg.getInt(JSON_KEY_TYPE)){
                    /*
                     *  Case Values and their corresponding operations
                     *      0   ==> Join
                     *      1   ==> Join Reply
                     *      2   ==> Change Successor(Sent to the predecessor when a new node joins)
                     *      3   ==> Insert
                     *      4   ==> Query
                     *      5   ==> Query Reply
                     *      6   ==> Delete
                     */

                    case JSON_TYPE_JOIN_REQUEST:
                        /*
                         *  Handles join operation of new nodes to the system.
                         *  The system is built such that a node join request is handled by the node
                         *  which will be the successor after the new node is in the system.
                         *  If a node is not its successor then it forwards the request to its
                         *  successor.
                         *  Multiple scenarios spawn when a node requests to join the system.
                         *
                         *  Scenario 1 :
                         *      Master node joins the system.
                         *      i.e. If NODE_ID(Successor) is null
                         *
                         *      Procedure:
                         *          Set the successor and predecessor to itself.
                         *
                         *  Scenario 2:
                         *      A node requests to join with only master node in the system.
                         *      i.e. NODE_ID(Successor) == NODE_ID(Predecessor)
                         *
                         *      Procedure:
                         *          Set NODE_ID(new) as the predecessor and successor of the master
                         *          node and reply with NODE_ID(current) as its predecessor &
                         *          successor.
                         *
                         *  Scenario 3:
                         *      A node requests to join and the current node is not its successor.
                         *      i.e. If NODE_ID(current) < NODE_ID(new)
                         *
                         *      Procedure:
                         *          Pass the request to successor.
                         *
                         *  Scenario 4:
                         *      A node requests join and the current node is its successor.
                         *      i.e If NODE_ID(current) > NODE_ID(new)
                         *      The procedure is followed only during the first instance of the
                         *      condition being satisfied.
                         *
                         *      Procedure:
                         *          1.  Send a change successor request to the current predecessor
                         *              with NODE_ID(new).
                         *          2.  Change PREDECESSOR(current) to NODE_ID(new).
                         *          3.  Send the required hash table values to NODE_ID(new).
                         */
                        if(SUCCESSOR_NODE_ID!=null){

                            String nodeHashValue = genHash(msg.getString(JSON_KEY_SOURCE_PORT));
                            if(PREDECESSOR_NODE_ID.compareTo(SUCCESSOR_NODE_ID)==0 &&
                                    SUCCESSOR_NODE_ID.equals(NODE_ID)){
                                /*
                                 *  If there is only one node in the DHT system, add the new node to
                                 *  the system and reply current node as it's predecessor and
                                 *  successor.
                                 */
                                SUCCESSOR_NODE_PORT = msg.getString(JSON_KEY_SOURCE_PORT);
                                PREDECESSOR_NODE_PORT = msg.getString(JSON_KEY_SOURCE_PORT);
                                SUCCESSOR_NODE_ID = genHash(SUCCESSOR_NODE_PORT);
                                PREDECESSOR_NODE_ID = genHash(PREDECESSOR_NODE_PORT);
                                /*
                                 * Creates a reply message & sends a reply.
                                 */
                                JSONObject reply = new JSONObject();
                                reply.put(JSON_KEY_TYPE,JSON_TYPE_JOIN_RESPONSE);
                                reply.put(JSON_KEY_PREDECESSOR,PORT);
                                reply.put(JSON_KEY_SUCCESSOR,PORT);

                                String[] clientMessage = {SUCCESSOR_NODE_PORT,reply.toString()};
                                new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                                                clientMessage);
                                Log.v("NODE JOIN SUCCESSFUL",
                                        SUCCESSOR_NODE_PORT+":"+PREDECESSOR_NODE_PORT);
                            }
                            else if(
                                    (NODE_ID.compareTo(PREDECESSOR_NODE_ID)>0 &&
                                        NODE_ID.compareTo(nodeHashValue)>0 &&
                                            PREDECESSOR_NODE_ID.compareTo(nodeHashValue)<0) ||
                                    (NODE_ID.compareTo(PREDECESSOR_NODE_ID)<0) &&
                                        (NODE_ID.compareTo(nodeHashValue)>0 ||
                                            PREDECESSOR_NODE_ID.compareTo(nodeHashValue)<0)){
                                JSONObject changeSuccessor = new JSONObject();
                                changeSuccessor.put(JSON_KEY_TYPE,JSON_TYPE_CHANGE_SUCCESSOR);
                                changeSuccessor.put(JSON_KEY_SUCCESSOR,
                                                        msg.get(JSON_KEY_SOURCE_PORT));

                                String[] clientMessage = {PREDECESSOR_NODE_PORT,
                                                            changeSuccessor.toString()};
                                new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                                    clientMessage);

                                JSONObject reply = new JSONObject();
                                reply.put(JSON_KEY_TYPE,JSON_TYPE_JOIN_RESPONSE);
                                reply.put(JSON_KEY_SUCCESSOR,PORT);
                                reply.put(JSON_KEY_PREDECESSOR,PREDECESSOR_NODE_PORT);

                                PREDECESSOR_NODE_PORT = msg.getString(JSON_KEY_SOURCE_PORT);
                                PREDECESSOR_NODE_ID = genHash(PREDECESSOR_NODE_PORT);
                                String[] newClientMessage = {PREDECESSOR_NODE_PORT,
                                                                        reply.toString()};
                                new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                                                newClientMessage);

                                /*
                                 *  Retrieving all data from the current node to be partitioned into
                                 *  the current node and the new node.
                                 *  Each row of data is checked and if necessary deleted from the
                                 *  current node and added to the message to be sent to the
                                 *  new node.
                                 */
                                JSONObject data = new JSONObject();
                                JSONObject query_result =
                                        query_local("@",null).getJSONObject(JSON_KEY_MESSAGE);
                                if(query_result != null){
                                    Iterator rowIterator = query_result.keys();
                                    while(rowIterator.hasNext()){
                                        String key = (String)rowIterator.next();
                                        if(PREDECESSOR_NODE_ID.compareTo(genHash(key))>0){
                                            String val = query_result.getString(key);
                                            data.put(key,val);
                                            delete(
                                                    URI,
                                                    HashTableContract.DHT.COLUMN_NAME_KEY +" = ?",
                                                    new String[]{key}
                                            );
                                        }
                                    }
                                    JSONObject transfer = new JSONObject();
                                    transfer.put(JSON_KEY_TYPE,JSON_TYPE_INSERT_VALUES);
                                    transfer.put(JSON_KEY_SOURCE_PORT,PORT);
                                    transfer.put(JSON_KEY_MESSAGE,data);
                                    String[] transferMessage = {PREDECESSOR_NODE_PORT,
                                            transfer.toString()};
                                    new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                            transferMessage);
                                    Log.v("NODE JOIN SUCCESSFUL",
                                            SUCCESSOR_NODE_PORT+":"+PREDECESSOR_NODE_PORT);
                                }
                            }
                            else{
                                String[] clientMessage = {SUCCESSOR_NODE_PORT,strings[0]};
                                new DHTClient().executeOnExecutor(
                                        AsyncTask.SERIAL_EXECUTOR,clientMessage);
                            }
                        }
                        else{
                            /*
                             *  When a new node joins, the DHT Network(i.e the Master Node),
                             *  sets the predecessor and successor to itself.
                             */
                            SUCCESSOR_NODE_PORT = msg.getString(JSON_KEY_SOURCE_PORT);
                            PREDECESSOR_NODE_PORT = msg.getString(JSON_KEY_SOURCE_PORT);
                            SUCCESSOR_NODE_ID = genHash(SUCCESSOR_NODE_PORT);
                            PREDECESSOR_NODE_ID = genHash(PREDECESSOR_NODE_PORT);
                        }

                        break;

                    case JSON_TYPE_JOIN_RESPONSE:

                        /*
                         *  Handles replies to Join Request.
                         *  Requires setting the NODE_ID(successor) & NODE_ID(predecessor).
                         *
                         */
                        SUCCESSOR_NODE_PORT = msg.getString(JSON_KEY_SUCCESSOR);
                        PREDECESSOR_NODE_PORT = msg.getString(JSON_KEY_PREDECESSOR);
                        SUCCESSOR_NODE_ID = genHash(SUCCESSOR_NODE_PORT);
                        PREDECESSOR_NODE_ID = genHash(PREDECESSOR_NODE_PORT);
                        break;

                    case JSON_TYPE_CHANGE_SUCCESSOR:

                        /*
                         *  Handles change of successor request.
                         *  Requires changing the NODE_ID(successor) to the new node.
                         */
                        SUCCESSOR_NODE_PORT = msg.getString(JSON_KEY_SUCCESSOR);
                        SUCCESSOR_NODE_ID = genHash(SUCCESSOR_NODE_PORT);
                        break;

                    case JSON_TYPE_INSERT_VALUES:
                        /*
                         *  Handles insert operations
                         *  Inserts to self if eligible or passes on to the successor.
                         *  Eligibility defined by comparing genHash(Message Key) to
                         *  genHash(Current Node)
                         */
                        if(msg.getString(JSON_KEY_SOURCE_PORT).compareTo(PORT)==0){
                            break;
                        }
                        JSONObject data = (JSONObject)msg.get(JSON_KEY_MESSAGE);
                        Iterator dataIterator = data.keys();
                        while(dataIterator.hasNext()){
                            /*
                             * Extracts key and checks if the genHash(key) is in the range of
                             * values that the current node handles.
                             * Handles whether to insert in the current node or pass it to
                             * the successor.
                             */
                            String key = (String)dataIterator.next();

                            /*
                             * Compares current node id to predecessor node it. If current node id
                             * is greater than predecessor node id, searches in the key space
                             * between the current and previous. i.e anything between the previous
                             * and current.
                             * If the current node id is less than predecessor, then searches the
                             * key space above the current node and also the key space below the
                             * predecessor.
                             */
                            if(
                                (NODE_ID.compareTo(PREDECESSOR_NODE_ID)>0 &&
                                    genHash(key).compareTo(NODE_ID)<=0 &&
                                        genHash(key).compareTo(PREDECESSOR_NODE_ID)>0)||
                                (NODE_ID.compareTo(PREDECESSOR_NODE_ID)<0 &&
                                    (genHash(key).compareTo(NODE_ID)<=0 ||
                                        genHash(key).compareTo(PREDECESSOR_NODE_ID)>0))
                            ){

                                ContentValues values = new ContentValues();
                                values.put(HashTableContract.DHT.COLUMN_NAME_KEY,key);
                                values.put(HashTableContract.DHT.COLUMN_NAME_VALUE,
                                                                    data.getString(key));
                                /*
                                 *  Inserting the <key,value> pair and deleting it from the message.
                                 */
                                insert(
                                        URI,
                                        values
                                );
                                data.remove(key);
                            }
                        }
                        if(data.length()!=0){
                            /*
                             *  If some rows have not been inserted they are passed on to the
                             *  successor.
                             *
                             *  Reconstructing the message
                             */
                            msg.put(JSON_KEY_MESSAGE,data);

                            String[] clientMessage = {SUCCESSOR_NODE_PORT,
                                    msg.toString()};
                            new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                                                clientMessage);
                        }
                        break;
                    case JSON_TYPE_QUERY_REQUEST_ALL:
                        if(msg.getString(JSON_KEY_SOURCE_PORT).compareTo(PORT)==0){
                            JSONObject response = msg.getJSONObject(JSON_KEY_MESSAGE);
                            query_response.add(response);
                        }
                        else {
                            String selection = msg.getString(JSON_KEY_QUERY);
                            JSONObject response;
                            if(msg.isNull(JSON_KEY_MESSAGE)){
                                response = new JSONObject();
                            }
                            else {
                                response = msg.getJSONObject(JSON_KEY_MESSAGE);
                            }
                            JSONObject local_response =
                                    query_local(selection, null).getJSONObject(JSON_KEY_MESSAGE);
                            Iterator query_all_iterator = local_response.keys();
                            while(query_all_iterator.hasNext()){
                                String key = (String)query_all_iterator.next();
                                String val = local_response.getString(key);
                                response.put(key,val);
                            }
                            msg.put(JSON_KEY_MESSAGE,response);
                            String[] clientMessage = {SUCCESSOR_NODE_PORT,msg.toString()};
                            new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                                                clientMessage);
                        }
                        break;
                    case JSON_TYPE_QUERY_REQUEST_KEY:
                        String selection = msg.getString(JSON_KEY_QUERY);
                        if(msg.getString(JSON_KEY_SOURCE_PORT).compareTo(PORT)==0){
                            JSONObject response = msg.getJSONObject(JSON_KEY_MESSAGE);
                            query_response.add(response);
                        }
                        else if(
                                (NODE_ID.compareTo(PREDECESSOR_NODE_ID)>0 &&
                                        PREDECESSOR_NODE_ID.compareTo(genHash(selection))<0 &&
                                        NODE_ID.compareTo(genHash(selection))>0) ||
                                        (NODE_ID.compareTo(PREDECESSOR_NODE_ID)<0 &&
                                                (NODE_ID.compareTo(genHash(selection))>0 ||
                                                        PREDECESSOR_NODE_ID.compareTo(genHash(selection))<0))
                                ){
                            JSONObject response = query_local(selection,null);
                            msg.put(JSON_KEY_MESSAGE,response);
                            String[] clientMessage = {SUCCESSOR_NODE_PORT,msg.toString()};
                            new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                                                                clientMessage);
                        }
                        else{
                            String[] clientMessage = {SUCCESSOR_NODE_PORT,msg.toString()};
                            new DHTClient().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,clientMessage);
                        }
                        break;
                    case JSON_TYPE_DELETE:
                        delete(
                                URI,
                                (String)msg.get(JSON_KEY_MESSAGE),
                                new String[]{}
                        );
                        break;
                }
            } catch (JSONException e) {
                Log.e(TAG,"JSONException: "+e.getMessage());
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG,"NoSuchAlgorithmException: "+e.getMessage());
            }
        }
    }
}