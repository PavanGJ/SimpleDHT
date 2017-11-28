package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by pavanjoshi on 4/1/17.
 */

public class HashTableContract {

    public static final String DATABASE_NAME = "DHTStorage.db";
    public static int DATABASE_VERSION = 1;

    private HashTableContract(){}

    public static class DHT{
        public static final String TABLE_NAME = "LocalHashTable";
        public static final String COLUMN_NAME_KEY = "key";
        public static final String COLUMN_NAME_VALUE = "value";
        public static final String SQL_CREATE_TABLE = "CREATE TABLE "+ TABLE_NAME + "(" +
                COLUMN_NAME_KEY + " TEXT PRIMARY KEY, " +
                COLUMN_NAME_VALUE + " TEXT)";
        public static final String SQL_DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE_NAME;
    }
}
