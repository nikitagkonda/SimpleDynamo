package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by nikita on 5/4/17.
 */

public class SimpleDynamoDatabaseHelper extends SQLiteOpenHelper {
    private static final String TABLE_NAME = "messages";
    private static final String DB_NAME = "gmDB";

    private static final String SQL_CREATE_TABLE = "CREATE TABLE " +
            TABLE_NAME +                       // Table's name
            "(" +                           // The columns in the table
            " key TEXT PRIMARY KEY, " +
            " value TEXT )";

    /*
     * Instantiates an open helper for the provider's SQLite data repository
     * Do not do database creation and upgrade here.
     */
    SimpleDynamoDatabaseHelper(Context context) {
        super(context, DB_NAME, null, 1);
    }
    /*
     * Creates the data repository. This is called when the provider attempts to open the
     * repository and SQLite reports that it doesn't exist.
     */
    public void onCreate(SQLiteDatabase db) {
        // Creates the table
        db.execSQL(SQL_CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int i, int i1) {
        if (i != i1) {
            db.execSQL("DROP TABLE IF EXISTS"+ TABLE_NAME);
            onCreate(db);
        }
    }
}
