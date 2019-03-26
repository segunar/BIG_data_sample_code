
//-------------------------------------
// IMPORTS
//-------------------------------------
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{ Level, Logger }

//-------------------------------------
// OBJECT MyProgram
//-------------------------------------
object MyProgram {

    //-------------------------------------
    // FUNCTION myMain
    //-------------------------------------
    def myMain(sc: SparkContext): Unit = {
        // 1. Operation C1: We parallelise the RDD
        val inputRDD = sc.parallelize(Array(1, 2, 3, 4))


        // 2. Operation A1: We collect the values
        val resVAL = inputRDD.collect()

        // 3. We print the result
        for (item <- resVAL) {
            println(item)
        }
    }

    // ------------------------------------------
    // PROGRAM MAIN ENTRY POINT
    // ------------------------------------------
    def main(args: Array[String]) {
        // 1. Local or Databricks
        val localFalseDatabricksTrue = false

        // 2. We configure the Spark Context sc
        var sc : SparkContext = null;

        // 2.1. Local mode
        if (localFalseDatabricksTrue == false){
            // 2.1.1. We create the configuration object
            val conf = new SparkConf()
            conf.setMaster("local")
            conf.setAppName("MyProgram")

            // 1.2. We initialise the Spark Context under such configuration
            sc = new SparkContext(conf)
        }
        else{
            sc = SparkContext.getOrCreate()
        }

        Logger.getRootLogger.setLevel(Level.WARN)
        for( index <- 1 to 10){
            printf("\n");
        }

        // 3. We call to myMain
        myMain(sc)
    }

}

//------------------------------------------------
// TRIGGER: Local (Comment) - Databricks (Enable)
//------------------------------------------------
//MyProgram.main(Array())
