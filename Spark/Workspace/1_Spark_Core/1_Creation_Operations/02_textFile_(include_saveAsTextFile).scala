
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{ Level, Logger }
import org.apache.commons.io.FileUtils
import java.io._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(sc: SparkContext, myDatasetDir: String, myResultDir: String): Unit = {
        // 1. Operation C1: Creation 'textFile', so as to store the content of the dataset into an RDD.
        val inputRDD = sc.textFile(myDatasetDir)

        // 2. Operation A1: Action 'saveAsTextFile', so as to store the content of inputRDD into the desired DBFS folder.
        inputRDD.saveAsTextFile(myResultDir)
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. Local or Databricks
        val localFalseDatabricksTrue = false

        // 2. We set the path to my_dataset and my_result
        val myLocalPath = "/home/nacho/CIT/Tools/MyCode/Spark/"
        val myDatabricksPath = "/"

        var myDatasetDir : String = "FileStore/tables/1_Spark_Core/my_dataset/"
        var myResultDir : String = "FileStore/tables/1_Spark_Core/my_result/"

        if (localFalseDatabricksTrue == false) {
            myDatasetDir = myLocalPath + myDatasetDir
            myResultDir = myLocalPath + myResultDir
        }
        else {
            myDatasetDir = myDatabricksPath + myDatasetDir
            myResultDir = myDatabricksPath + myResultDir
        }

        // 3. We remove my_result directory
        if (localFalseDatabricksTrue == false) {
            FileUtils.deleteDirectory(new File(myResultDir))
        }
        else {
            dbutils.fs.rm(myResultDir, true)
        }

        // 4. We configure the Spark Context sc
        var sc : SparkContext = null;

        // 4.1. Local mode
        if (localFalseDatabricksTrue == false){
            // 4.1.1. We create the configuration object
            val conf = new SparkConf()
            conf.setMaster("local")
            conf.setAppName("MyProgram")

            // 4.1.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        // 4.2. Databricks Mode
        else{
            sc = SparkContext.getOrCreate()
        }

        Logger.getRootLogger.setLevel(Level.WARN)
        for( index <- 1 to 10){
            printf("\n")
        }

        // 5. We call to myMain
        myMain(sc, myDatasetDir, myResultDir)
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
//MyProgram.main(Array())
