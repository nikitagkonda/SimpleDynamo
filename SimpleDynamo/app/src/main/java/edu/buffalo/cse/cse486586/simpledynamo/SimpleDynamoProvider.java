package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Formatter;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String[] portNumbers =  new String[]{ "11108","11112","11116","11120","11124"};
	static final int SERVER_PORT = 10000;
	static String myPort;
	private static final String KEY = "key";
	private static final String VALUE = "value";
	private static final String TABLE_NAME = "messages";

	static ArrayList<Nodes> nodeRing = new ArrayList<Nodes>();

	static Comparator<Nodes> comparator = new Comparator<Nodes>() {
		public int compare(Nodes n1, Nodes n2) {
			return n1.getId().compareTo(n2.getId());
		}
	};

	private class Nodes {

		String id;
		String port_no;

		public Nodes(String id, String port_no) {
			this.id = id;
			this.port_no = port_no;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getPort_no() {
			return port_no;
		}

		public void setPort_no(String port_no) {
			this.port_no = port_no;
		}
	}

	{
		if(nodeRing.size()>=0 && nodeRing.size() < 5){
			nodeRing.clear();
			for (String pno : portNumbers) {
				String node_id = null;
				try {
					node_id = genHash(String.valueOf(Integer.parseInt(pno) / 2));
					Nodes node = new Nodes(node_id, pno);
					nodeRing.add(node);
					Log.d(TAG, "doInBackground: node Ring"+ pno);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}

			}
			Collections.sort(nodeRing, comparator);
		}
	}
    /*
    *Reference: https://developer.android.com/guide/topics/providers/content-provider-creating.html
    */

	private SimpleDynamoDatabaseHelper dbHelper;

	// Holds the database object
	private SQLiteDatabase db;

	private final Uri mUri= buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}



	//static boolean replicateFlag = false;
	//static boolean deleteReplicaFlag = false;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		db =dbHelper.getWritableDatabase();

		if(selection.equals("@")){
			db.delete(TABLE_NAME, null,null);
		} else if(selection.equals("*")){
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteall");
		} else {

				String keyHash = null;
				try {
					keyHash = genHash(selection);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				boolean deleteFlag=false;
				for (int i=0;i<nodeRing.size();i++) {
					if(i==0){
						if(keyHash.compareTo(nodeRing.get(i).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
							deleteFlag =true;
						}
					}
					else if (keyHash.compareTo(nodeRing.get(i).getId()) <= 0 && keyHash.compareTo(nodeRing.get(i-1).getId())>0){
						deleteFlag=true;
					}

					if(deleteFlag) {

						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "delete", nodeRing.get(i).getPort_no(), selection);

						//delete replicas
						for(int j=i+1;j<=i+2;j++){
							int k = j;
							if(j>=nodeRing.size()){
								k = j-nodeRing.size();
							}
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteReplicas", nodeRing.get(k).getPort_no(), selection);
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

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub


		 /*
        Reference 1: https://developer.android.com/reference/android/database/sqlite/SQLiteQueryBuilder.html
        Reference 2: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html
         */

		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		db = dbHelper.getWritableDatabase();

		String keyHash = null;
		try {
			keyHash = genHash(values.getAsString(KEY));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		boolean insertFlag=false;

		for (int i=0;i<nodeRing.size();i++) {
			if(i==0){
				if(keyHash.compareTo(nodeRing.get(i).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
					insertFlag =true;
				}
			}
			else if (keyHash.compareTo(nodeRing.get(i).getId()) <= 0 && keyHash.compareTo(nodeRing.get(i-1).getId())>0){
				insertFlag=true;
			}
			if(insertFlag) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", nodeRing.get(i).getPort_no(), values.getAsString(KEY), values.getAsString(VALUE));
				//replicate
				for(int j=i+1;j<=i+2;j++){
                    int k = j;
                    if(j>=nodeRing.size()){
                        k = j-nodeRing.size();
                    }
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "replicate", nodeRing.get(k).getPort_no(), values.getAsString(KEY), values.getAsString(VALUE));
                }
				break;
			}

		}
//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		/*nodeRing.clear();
		for (String pno : portNumbers) {
			String node_id = null;
			try {
				node_id = genHash(String.valueOf(Integer.parseInt(pno) / 2));
				Nodes node = new Nodes(node_id, pno);
				nodeRing.add(node);
				Log.d(TAG, "doInBackground: node Ring"+ pno);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}

		}*/
		//Collections.sort(nodeRing, comparator);

		//Reference: https://developer.android.com/guide/topics/providers/content-provider-creating.html
		dbHelper = new SimpleDynamoDatabaseHelper(
				getContext()       // the application context
		);

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

			Log.e(TAG, "Can't create a ServerSocket for port"+myPort);
		}

		String[] result = null;
		try {
			result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "recover", myPort).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		if (result != null) {
			for(String r : result){
				String[] temp = r.split(",");
				//insert temp
				ContentValues values= new ContentValues();
				values.put("key",temp[0]);
				values.put("value",temp[1]+","+temp[2]);

				//insert directly
				db =dbHelper.getWritableDatabase();
				SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
				queryBuilder.setTables(TABLE_NAME);
				queryBuilder.appendWhere(KEY+"='"+values.getAsString(KEY)+"'");
				Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
				boolean updateFlag = false;
				String old_v="";
				while (cursor.moveToNext()) {
					old_v = cursor.getString(1);
					updateFlag = true;
				}
				cursor.close();





				if (updateFlag) {
//					Log.d(TAG, "onCreate: update");
					String[] new_val = values.getAsString(VALUE).split(",");
					String[] old_val = old_v.split(",");
					int new_version = Integer.parseInt(new_val[1]);
					int old_version = Integer.parseInt(old_val[1]);
//					Log.d(TAG, "onCreate: new version: "+new_val[1]+", old version: "+ old_val[1]);
					if(new_version>=old_version){
						int updaterow = db.update(TABLE_NAME, values, KEY + "='" + values.getAsString(KEY) + "'", null);
//						Log.d(TAG, "recovery update: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE));
					}

				} else {
					long id = db.insert(TABLE_NAME, null, values);
//					Log.d(TAG, "recovery new: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE));
				}
			}
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.d(TAG, "query: start "+selection);
		if(nodeRing.size() < 5){
			try {
			Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
//		try {
//			Thread.sleep(150);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		/*
        Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteQueryBuilder.html
         */

		db=dbHelper.getReadableDatabase();
		SQLiteQueryBuilder queryBuilder= new SQLiteQueryBuilder();
		queryBuilder.setTables(TABLE_NAME);

		/*if((nodeRing.size()==0 || nodeRing.size()==1)){
			if(!selection.equals("*") && !selection.equals("@"))
				queryBuilder.appendWhere(KEY+"='"+selection+"'");
			return queryBuilder.query(db,projection,null,selectionArgs,null,null,null);
		}*/

		if(selection.equals("@"))
		{
			Cursor cursor =  queryBuilder.query(db,projection,null,selectionArgs,null,null,null);
			MatrixCursor new_cursor = new MatrixCursor(new String[]{KEY, VALUE});
			while (cursor.moveToNext()){
				String k = cursor.getString(0);
				String[] v = cursor.getString(1).split(",");
				new_cursor.addRow(new Object[]{k, v[0]});
			}
			return new_cursor;
		}

		else if(!selection.equals("*")){
			queryBuilder.appendWhere(KEY+"='"+selection+"'");
			String keyHash=null;
			try {
				keyHash=genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			boolean queryFlag=false;
			for (int i=0;i<nodeRing.size();i++) {
				if(i==0){
					if(keyHash.compareTo(nodeRing.get(i).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
						queryFlag =true;

					}
				}
				else if (keyHash.compareTo(nodeRing.get(i).getId()) <= 0 && keyHash.compareTo(nodeRing.get(i-1).getId())>0){
					queryFlag=true;
				}

				if(queryFlag) {
					/*if (myPort.equals(nodeRing.get(i).getPort_no())) {

						//                        Log.v("return",String.valueOf(cursor.getCount()));
						return queryBuilder.query(db,projection,null,selectionArgs,null,null,null);
					}
					else {*/
					//Log.d(TAG, "query: start "+selection);

					String[] result = null;
					MatrixCursor cursor = new MatrixCursor(new String[]{KEY, VALUE});
					try {
//						while (result==null || result[1]==null) {
//
//							result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", nodeRing.get(i).getPort_no(), selection).get();
//							if (result != null && result[1] != null) {
//								cursor.addRow(new Object[]{result[0], result[1]});
//							} else {
//								//query replicas
//								for (int j = i + 1; j <= i + 2; j++) {
//									int k = j;
//									if (j >= nodeRing.size()) {
//										k = j - nodeRing.size();
//									}
//									Log.d(TAG, "query: replicas");
//									result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", nodeRing.get(k).getPort_no(), selection).get();
//									if (result != null && result[1]!=null) {
//										cursor.addRow(new Object[]{result[0], result[1]});
//										break;
//									}
//								}
//							}
//
//
//
//						}
						String value1;
						String value2;
						int version1=0;
						int version2=0;
						String v1[] = null;
						String v2[] = null;

//						while(!value1.equals(value2)){
							int count = 0;
							while (count == 0){
								result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", nodeRing.get(i).getPort_no(), selection).get();
								if (result != null && result[1] != null) {
									value1 = result[1];
									v1 = value1.split(",");
									version1 = Integer.parseInt(v1[1]);
									//cursor.addRow(new Object[]{result[0], result[1]});
									count++;

								}
								for (int j = i + 1; j <= i + 2; j++) {
									int k = j;
									if (j >= nodeRing.size()) {
										k = j - nodeRing.size();
									}
									Log.d(TAG, "query: replicas");
									result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "query", nodeRing.get(k).getPort_no(), selection).get();
									if (result != null && result[1]!=null) {
										if(count == 0){
											value1 = result[1];
											v1 = value1.split(",");
											version1 = Integer.parseInt(v1[1]);
											count++;
										}
										else{
											String[] tempv2 = result[1].split(",");
											if(Integer.parseInt(tempv2[1]) >= version2) {
												value2 = result[1];
												v2 = value2.split(",");
												version2 = Integer.parseInt(v2[1]);
												count++;
											}
											//break;
										}

										//cursor.addRow(new Object[]{result[0], result[1]});

									}
								}
							}
//						}
						if(version2 == 0 || version1 >= version2){
							cursor.addRow(new Object[]{selection, v1[0]});
							Log.d(TAG, "query: key "+selection+ " , result: "+v1[0]);
						}
						else if(version2 > version1){
							cursor.addRow(new Object[]{selection, v2[0]});
							Log.d(TAG, "query: key "+selection+ " , result: "+v2[0]);
						}

						return cursor;

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
					//}
					}

			}
		} else if(selection.equals("*")){

			String[] result = null;

			try {
				result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryall").get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			MatrixCursor cursor = new MatrixCursor(new String[] { KEY, VALUE });
			if (result != null) {
				for(String r : result){
					String[] temp = r.split(",");
					cursor.addRow(new Object[] { temp[0], temp[1]});
				}
			}

			return cursor;

		}

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
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

			while(true) {
				try {
					Socket server = serverSocket.accept();
					ObjectInputStream ois = new ObjectInputStream(server.getInputStream());
					Object o = ois.readObject();
					String[] received = (String[]) o;

					if(received[0].equals("insert")){
						ContentValues values= new ContentValues();
						values.put("key",received[2]);
						values.put("value",received[3]);

						db =dbHelper.getWritableDatabase();
						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(TABLE_NAME);
						queryBuilder.appendWhere(KEY+"='"+values.getAsString(KEY)+"'");
						Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
						boolean updateFlag = false;
						String old_v="";
						while (cursor.moveToNext()) {
							old_v = cursor.getString(1);
							updateFlag = true;
						}
						cursor.close();





						if (updateFlag) {
							String[] old_val = old_v.split(",");
							int old_version = Integer.parseInt(old_val[1]);
							old_version++;
							String new_version = String.valueOf(old_version);
							String new_val = values.getAsString(VALUE)+","+new_version;
							values.put(VALUE,new_val);
							int updaterow = db.update(TABLE_NAME, values, KEY + "='" + values.getAsString(KEY) + "'", null);
//							try {
//								Log.d(TAG, "insert update: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE)+ "3) keyhash: "+genHash(values.getAsString(KEY)));
//							} catch (NoSuchAlgorithmException e) {
//								e.printStackTrace();
//							}

						} else {
							String new_version = "1";
							String new_val = values.getAsString(VALUE)+","+new_version;
							values.put("value",new_val);
							long id=-1;
							while(id==-1){
								id = db.insert(TABLE_NAME, null, values);
							}
//							try {
//								Log.d(TAG, "insert new: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE)+ "3) keyhash: "+genHash(values.getAsString(KEY)));
//							} catch (NoSuchAlgorithmException e) {
//								e.printStackTrace();
//							}
						}

//						//insert directly
//						db =dbHelper.getWritableDatabase();
//						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
//						queryBuilder.setTables(TABLE_NAME);
//						Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
//						boolean updateFlag = false;
//						while (cursor.moveToNext()) {
//							String s = cursor.getString(0);
//							if (s.equals(values.getAsString(KEY))) {
//								updateFlag = true;
//								break;
//							}
//						}
//
//						Log.d(TAG, "servertask: insert: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE)+ "3) keyhash: "+genHash(values.getAsString(KEY)));
//
//						cursor.close();
//						if (updateFlag) {
//							int updaterow = db.update(TABLE_NAME, values, KEY + "='" + values.getAsString(KEY) + "'", null);
//						} else {
//							long id=-1;
//							while(id==-1){
//								id = db.insert(TABLE_NAME, null, values);
//							}
//						}
						//Uri newUri = getContext().getContentResolver().insert(mUri, values);
						//Log.d(TAG, "doInBackground: insert  1) key: "+received[2]+" 2) value: "+received[3]);

					}

					else if(received[0].equals("replicate")){
						//replicateFlag = true;
						ContentValues values= new ContentValues();
						values.put("key",received[2]);
						values.put("value",received[3]);

						db =dbHelper.getWritableDatabase();
						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
						queryBuilder.setTables(TABLE_NAME);
						queryBuilder.appendWhere(KEY+"='"+values.getAsString(KEY)+"'");
						Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
						boolean updateFlag = false;
						String old_v="";
						while (cursor.moveToNext()) {
							old_v = cursor.getString(1);
							updateFlag = true;
						}
						cursor.close();





						if (updateFlag) {
							String[] old_val = old_v.split(",");
							int old_version = Integer.parseInt(old_val[1]);
							old_version++;
							String new_version = String.valueOf(old_version);
							String new_val = values.getAsString(VALUE)+","+new_version;
							values.put(VALUE,new_val);
							int updaterow = db.update(TABLE_NAME, values, KEY + "='" + values.getAsString(KEY) + "'", null);
//							try {
//								Log.d(TAG, "replicate update: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE)+ "3) keyhash: "+genHash(values.getAsString(KEY)));
//							} catch (NoSuchAlgorithmException e) {
//								e.printStackTrace();
//							}

						} else {
							String new_version = "1";
							String new_val = values.getAsString(VALUE)+","+new_version;
							values.put("value",new_val);
							long id=-1;
							while(id==-1){
								id = db.insert(TABLE_NAME, null, values);
							}
//							try {
//								Log.d(TAG, "replicate new: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE)+ "3) keyhash: "+genHash(values.getAsString(KEY)));
//							} catch (NoSuchAlgorithmException e) {
//								e.printStackTrace();
//							}
						}

						//insert directly
//						db =dbHelper.getWritableDatabase();
//						SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
//						queryBuilder.setTables(TABLE_NAME);
//						Cursor cursor = queryBuilder.query(db, null, null, null, null, null, null);
//						boolean updateFlag = false;
//						while (cursor.moveToNext()) {
//							String s = cursor.getString(0);
//							if (s.equals(values.getAsString(KEY))) {
//								updateFlag = true;
//								break;
//							}
//						}
//
//						Log.d(TAG, "servertask: replicate: 1) key: "+values.getAsString(KEY)+" 2) value: "+values.getAsString(VALUE)+ "3) keyhash: "+genHash(values.getAsString(KEY)));
//
//						cursor.close();
//						if (updateFlag) {
//							int updaterow = db.update(TABLE_NAME, values, KEY + "='" + values.getAsString(KEY) + "'", null);
//						} else {
//							long id=-1;
//							while(id==-1){
//								id = db.insert(TABLE_NAME, null, values);
//							}
//						}
						//Uri newUri = getContext().getContentResolver().insert(mUri, values);
						//replicateFlag = false;
						//Log.d(TAG, "doInBackground: replicate  1) key: "+received[2]+" 2) value: "+received[3]);
					}

					else if(received[0].equals("query")){
//						Cursor resultCursor = getContext().getContentResolver().query(mUri, null,
//								received[2], null, null);
						db=dbHelper.getReadableDatabase();
						SQLiteQueryBuilder queryBuilder= new SQLiteQueryBuilder();
						queryBuilder.setTables(TABLE_NAME);
						queryBuilder.appendWhere(KEY+"='"+received[2]+"'");
						Cursor resultCursor = queryBuilder.query(db,null,null,null,null,null,null);

						String[] result = new String[2];
						while(resultCursor.moveToNext()){
							result[0]= resultCursor.getString(0);
							//String[] arr = resultCursor.getString(1).split(",");
							result[1] =  resultCursor.getString(1);
						}

						resultCursor.close();

						ObjectOutputStream oos = new ObjectOutputStream(server.getOutputStream());
						oos.writeObject(result);


					}
					else if(received[0].equals("queryall")){
						Cursor resultCursor = getContext().getContentResolver().query(mUri, null,
								"@", null, null);
						ArrayList<String> listResult = new ArrayList<String>();

						while(resultCursor.moveToNext()){
							String result= resultCursor.getString(0) + "," + resultCursor.getString(1);
							listResult.add(result);
						}

						resultCursor.close();

						String [] result = listResult.toArray(new String[listResult.size()]);

						ObjectOutputStream oos = new ObjectOutputStream(server.getOutputStream());
						oos.writeObject(result);
					}
					else if(received[0].equals("deleteall")){
						int i = getContext().getContentResolver().delete(mUri,"@",null);
					}
					else if(received[0].equals("delete")){
						//int i = getContext().getContentResolver().delete(mUri,received[2],null);
						//delete directly
						db = dbHelper.getWritableDatabase();
						db.delete(TABLE_NAME, KEY + "='" + received[2] + "'", null);
					}
					else if(received[0].equals("deleteReplicas")){
						//deleteReplicaFlag = true;
						//int i = getContext().getContentResolver().delete(mUri,received[2],null);
						//deleteReplicaFlag = false;

						//delete directly
						db = dbHelper.getWritableDatabase();
						db.delete(TABLE_NAME, KEY + "='" + received[2] + "'", null);
					}
					else if(received[0].equals("recover")){
						if(received[2].equals("predecessor")){
							db = dbHelper.getReadableDatabase();
							SQLiteQueryBuilder queryBuilder= new SQLiteQueryBuilder();
							queryBuilder.setTables(TABLE_NAME);
							Cursor resultCursor =  queryBuilder.query(db,null,null,null,null,null,null);
							ArrayList<String> listResult = new ArrayList<String>();
							Integer[]  p = new Integer[2];
							p[0] = Integer.parseInt(received[3]);
							p[1] = Integer.parseInt(received[4]);
							while(resultCursor.moveToNext()){
								String keyHash = genHash(resultCursor.getString(0));
								boolean insertFlag = false;
								for(Integer i : p){
									if(i==0){
										if(keyHash.compareTo(nodeRing.get(i).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
											insertFlag =true;

										}
									}
									else if (keyHash.compareTo(nodeRing.get(i).getId()) <= 0 && keyHash.compareTo(nodeRing.get(i-1).getId())>0){
										insertFlag=true;
									}
									if(insertFlag){
										String result= resultCursor.getString(0) + "," + resultCursor.getString(1);
										listResult.add(result);
										break;
									}
								}

							}

							resultCursor.close();

							String [] result = listResult.toArray(new String[listResult.size()]);

							ObjectOutputStream oos = new ObjectOutputStream(server.getOutputStream());
							oos.writeObject(result);
						}
						else if(received[2].equals("successor")){
							db = dbHelper.getReadableDatabase();
							SQLiteQueryBuilder queryBuilder= new SQLiteQueryBuilder();
							queryBuilder.setTables(TABLE_NAME);
							Cursor resultCursor =  queryBuilder.query(db,null,null,null,null,null,null);
							ArrayList<String> listResult = new ArrayList<String>();
							int s = Integer.parseInt(received[3]);
							while(resultCursor.moveToNext()){
								String keyHash = genHash(resultCursor.getString(0));
								boolean insertFlag = false;

								if(s==0){
									if(keyHash.compareTo(nodeRing.get(s).getId()) <= 0 || keyHash.compareTo(nodeRing.get(nodeRing.size()-1).getId())>0) {
										insertFlag =true;
									}
								}
								else if (keyHash.compareTo(nodeRing.get(s).getId()) <= 0 && keyHash.compareTo(nodeRing.get(s-1).getId())>0){
									insertFlag=true;
								}
								if(insertFlag){
									String result= resultCursor.getString(0) + "," + resultCursor.getString(1);
									listResult.add(result);
								}

							}

							resultCursor.close();
							String [] result = listResult.toArray(new String[listResult.size()]);
							ObjectOutputStream oos = new ObjectOutputStream(server.getOutputStream());
							oos.writeObject(result);

						}

					}

				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		}

		protected void onProgressUpdate(String...strings) {

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
	private class ClientTask extends AsyncTask<String, Void, String[]> {

		@Override
		protected String[] doInBackground(String... msgs) {

				if(msgs[0].equals("insert")){

					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[1]));
					} catch (IOException e) {
						e.printStackTrace();
					}

					ObjectOutputStream oos = null;
					try {
						if (socket != null) {
							oos = new ObjectOutputStream(socket.getOutputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (oos != null) {
							oos.writeObject(msgs);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						if (socket != null) {
							socket.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}


				}

				else if(msgs[0].equals("replicate")){

					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[1]));
					} catch (IOException e) {
						e.printStackTrace();
					}

					ObjectOutputStream oos = null;
					try {
						if (socket != null) {
							oos = new ObjectOutputStream(socket.getOutputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (oos != null) {
							oos.writeObject(msgs);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						if (socket != null) {
							socket.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

				}

				else if(msgs[0].equals("query")){
					Socket socket;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[1]));
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					}

					ObjectOutputStream oos;
					try {
						oos = new ObjectOutputStream(socket.getOutputStream());
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					}
					try {
						oos.writeObject(msgs);
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					}
//					try {
//						Thread.sleep(100);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}

					ObjectInputStream ois;
					try {
						ois = new ObjectInputStream(socket.getInputStream());
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					}
					String[] result = new String[0];
					try {
						result = (String[]) ois.readObject();
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
					try {
						socket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					return result;

				}
				else if(msgs[0].equals("queryall")){

					ArrayList<String[]> listResult = new ArrayList<String[]>();
					for(int i=0;i<nodeRing.size();i++) {

						Socket socket = null;
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodeRing.get(i).getPort_no()));
						} catch (IOException e) {
							e.printStackTrace();
						}

						ObjectOutputStream oos = null;
						try {
							if (socket != null) {
								oos = new ObjectOutputStream(socket.getOutputStream());
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							if (oos != null) {
								oos.writeObject(msgs);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
//						try {
//							Thread.sleep(50);
//						} catch (InterruptedException e) {
//							e.printStackTrace();
//						}

						ObjectInputStream ois = null;
						try {
							if (socket != null) {
								ois = new ObjectInputStream(socket.getInputStream());
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							if (ois != null) {
								listResult.add((String[]) ois.readObject());
							}
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						try {
							if (socket != null) {
								socket.close();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}

					}

					ArrayList<String> temp = new ArrayList<String>();

					for(String[] x: listResult){
						Collections.addAll(temp, x);
					}
					return temp.toArray(new String[temp.size()]);
				}
				else if(msgs[0].equals("deleteall")){
					for(int i=0;i<nodeRing.size();i++) {

						Socket socket = null;
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(nodeRing.get(i).getPort_no()));
						} catch (IOException e) {
							e.printStackTrace();
						}

						ObjectOutputStream oos = null;
						try {
							if (socket != null) {
								oos = new ObjectOutputStream(socket.getOutputStream());
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							if (oos != null) {
								oos.writeObject(msgs);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						try {
							if (socket != null) {
								socket.close();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}

					}
				}
				else if(msgs[0].equals("delete")){
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[1]));
					} catch (IOException e) {
						e.printStackTrace();
					}

					ObjectOutputStream oos = null;
					try {
						if (socket != null) {
							oos = new ObjectOutputStream(socket.getOutputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (oos != null) {
							oos.writeObject(msgs);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						if (socket != null) {
							socket.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				else if(msgs[0].equals("deleteReplicas")){
					Socket socket = null;
					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[1]));
					} catch (IOException e) {
						e.printStackTrace();
					}

					ObjectOutputStream oos = null;
					try {
						if (socket != null) {
							oos = new ObjectOutputStream(socket.getOutputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (oos != null) {
							oos.writeObject(msgs);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						if (socket != null) {
							socket.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				else if(msgs[0].equals("recover")){
					ArrayList<String[]> listResult = new ArrayList<String[]>();
					int p1 =0;
					int p2 =0;
					int s = 0;
					int r = 0;
					for(int i=0;i<nodeRing.size();i++) {
						if (nodeRing.get(i).getPort_no().equals(myPort)) {
							if (i == 0) {
								p1 = nodeRing.size() - 1;
								p2 = nodeRing.size() - 2;

							} else  if(i==1){
								p1 = i - 1;
								p2 = nodeRing.size() -1;
							} else{
								p1 = i-1;
								p2 = i-2;
							}

							if (i != nodeRing.size() - 1)
								s =i+1;
							else
								s = 0;
							r =i;
							break;
						}
					}
					String[] msg = new String[5];
					msg[0] = msgs[0];
					msg[1] = msgs[1];
					Socket socket1 = null;
					try {
						socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodeRing.get(p1).getPort_no()));
					} catch (IOException e) {
						e.printStackTrace();
					}

					msg[2] = "predecessor";
					msg[3] = String.valueOf(p1);
					msg[4] = String.valueOf(p2);
					Log.d(TAG, "doInBackground: "+msg[3]+ " "+msg[4]);
					ObjectOutputStream oos1 = null;
					try {
						if (socket1 != null) {
							oos1 = new ObjectOutputStream(socket1.getOutputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (oos1 != null) {
							oos1.writeObject(msg);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
//					try {
//						Thread.sleep(10);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}

					ObjectInputStream ois1 = null;
					try {
						if (socket1 != null) {
							ois1 = new ObjectInputStream(socket1.getInputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (ois1 != null) {
							listResult.add((String[]) ois1.readObject());
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
					try {
						if (socket1 != null) {
							socket1.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

					Socket socket2 = null;
					try {
						socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(nodeRing.get(s).getPort_no()));
					} catch (IOException e) {
						e.printStackTrace();
					}

					msg[2] = "successor";
					msg[3] = String.valueOf(r);
					ObjectOutputStream oos2 = null;
					try {
						if (socket2 != null) {
							oos2 = new ObjectOutputStream(socket2.getOutputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (oos2 != null) {
							oos2.writeObject(msg);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
//					try {
//						Thread.sleep(10);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}

					ObjectInputStream ois2 = null;
					try {
						if (socket2 != null) {
							ois2 = new ObjectInputStream(socket2.getInputStream());
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if (ois2 != null) {
							listResult.add((String[]) ois2.readObject());
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
					try {
						if (socket2 != null) {
							socket2.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

					//}

					ArrayList<String> temp = new ArrayList<String>();

					for(String[] x: listResult){
						Collections.addAll(temp, x);
					}
					return temp.toArray(new String[temp.size()]);
				}



			return null;
		}
	}
}


