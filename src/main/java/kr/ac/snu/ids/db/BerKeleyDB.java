package kr.ac.snu.ids.db;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;

public class BerKeleyDB {

    private static Database INSTANCE = null;
    private static Environment envDb = null;

    public static Database getInstance() {
        if (INSTANCE == null) {
            synchronized (BerKeleyDB.class) {
                if (INSTANCE == null) {
                    EnvironmentConfig envConfig = new EnvironmentConfig();
                    envConfig.setAllowCreate(true);
                    envDb = new Environment(new File("db/"), envConfig);

                    DatabaseConfig dbConfig = new DatabaseConfig();
                    dbConfig.setAllowCreate(true);
                    dbConfig.setSortedDuplicates(true);

                    INSTANCE = envDb.openDatabase(null, "sampleDatabase", dbConfig);
                }
            }
        }
        return INSTANCE;
    }

    public static synchronized void closeDatabase() {
        if (INSTANCE != null) {
            INSTANCE.close();
            envDb.close();
            INSTANCE = null;
            envDb = null;
        }
    }
}
