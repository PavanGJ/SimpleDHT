package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by pavanjoshi on 4/1/17.
 */

public class HashTable extends SQLiteOpenHelper {

    public HashTable(Context context){
        super(context,HashTableContract.DATABASE_NAME,null,HashTableContract.DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(HashTableContract.DHT.SQL_CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(HashTableContract.DHT.SQL_DROP_TABLE);
        db.execSQL(HashTableContract.DHT.SQL_CREATE_TABLE);
        HashTableContract.DATABASE_VERSION++;
    }
}
