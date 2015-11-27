//run_ctr_predict

import scala.math.{abs, max, min, log}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object CTRPredict {
    def main(args: Array[String]) {

    	if (args.length != 1) {
    	  println("Usage: /path/to/spark/bin/spark-submit --driver-memory 2g --class CTRPredict " +
        	"target/scala-*/ctrpredict_2.10-0.1.jar data/sample.csv")
      		sys.exit(1)
    	}

    	val conf = (new SparkConf().setMaster("local")
    								.setAppName("CTR Prediction")
    								.set("spark.executor.memory", "6g")
    								.set("spark.driver.memory", "6g"))

		val sc = new SparkContext(conf)   
	
		var inputPath =  args(0)
		var ctrRDD = sc.textFile(inputPath);
		println("Total records : " + ctrRDD.count)

		var train_test_rdd = ctrRDD.randomSplit(Array(0.8, 0.2), seed = 37L)

		var train_raw_rdd = train_test_rdd(0)
		var test_raw_rdd = train_test_rdd(1)

		println("Train records : " + train_raw_rdd.count)
		println("Test records : " + test_raw_rdd.count)

		//cache train, test
		train_raw_rdd.cache()
		test_raw_rdd.cache()
	
		
		var train_rdd = train_raw_rdd.map{ line =>
						var tokens = line.split(",")
						var catkey = tokens(0) + "::" + tokens(1)
						var catfeatures = tokens.slice(5, 14)
						var numericalfeatures = tokens.slice(15, tokens.size-1)
						(catkey, catfeatures, numericalfeatures)
						}

		println(train_rdd.take(1)(0))

		
		// encode categorical data using 1-k encoding			
		var train_cat_rdd  = train_rdd.map{x => 
											parseCatFeatures(x._2)
											}
		
		train_cat_rdd.first.foreach(println)
		
		var oheMap = train_cat_rdd.flatMap(x => x).distinct().zipWithIndex().collectAsMap()

		//println(oheMap get (0,"1fbe01fe"))
		println(oheMap get (0,"1fbe01fe"))

		println("Number of features")
		println(oheMap.size)

		//create OHE for train data			
		var ohe_train_rdd = train_rdd.map{ case (key, cateorical_features, numerical_features) =>

				  		var cat_features_indexed = parseCatFeatures(cateorical_features)				  				  		
				  		var cat_feature_ohe = new ArrayBuffer[Double]
				  		for (k <- cat_features_indexed) {
				  			if(oheMap contains k){
								cat_feature_ohe += (oheMap get (k)).get.toDouble
				  			}else {
				  				cat_feature_ohe += 0.0
				  			}				  			
				  		}

				  		var numerical_features_dbl  = numerical_features.map{x => 
				  											var x1 = if (x.toInt < 0) "0" else x
				  											x1.toDouble}
				  		var features = cat_feature_ohe.toArray ++  numerical_features_dbl						
				  		LabeledPoint(key.split("::")(1).toInt, Vectors.dense(features))      				  						  						  		
						}

		ohe_train_rdd.take(10).foreach(println)
		
		//train Naive Bayes model
		var naiveBayesModel = NaiveBayes.train(ohe_train_rdd)

		//create OHE for test data
		var test_rdd = test_raw_rdd.map{ line =>
						var tokens = line.split(",")
						var catkey = tokens(0) + "::" + tokens(1)
						var catfeatures = tokens.slice(5, 14)
						var numericalfeatures = tokens.slice(15, tokens.size-1)
						(catkey, catfeatures, numericalfeatures)
						}		

		var ohe_test_rdd = test_rdd.map{ case (key, cateorical_features, numerical_features) =>

				  		var cat_features_indexed = parseCatFeatures(cateorical_features)				  				  		
				  		var cat_feature_ohe = new ArrayBuffer[Double]
				  		for (k <- cat_features_indexed) {				  			
				  			if(oheMap contains k){
				  				cat_feature_ohe += (oheMap get (k)).get.toDouble
				  			}else {
				  				cat_feature_ohe += 0.0
				  			}
				  		}

						var numerical_features_dbl  = numerical_features.map{x => 
				  											var x1 = if (x.toInt < 0) "0" else x
				  											x1.toDouble}
				  		//var numerical_features_dbl  = numerical_features.map(x => x.toDouble)

				  		var features = cat_feature_ohe.toArray ++  numerical_features_dbl						
				  		LabeledPoint(key.split("::")(1).toInt, Vectors.dense(features))      				  						  						  		
						}		

		//mode base accuracy						
		var predictions_base = ohe_test_rdd.map(lp => 1.0)
		predictions_base.take(10).foreach(println)
		var predictionAndLabelBase = predictions_base.zip( ohe_test_rdd.map(_.label))
		var accuracy_base = 1.0 * predictionAndLabelBase.filter(x => x._1 == x._2 ).count/ohe_test_rdd.count
		println("Base Model accuracy " + accuracy_base)

		// actual mode accuracy
		var predictions = ohe_test_rdd.map(lp => naiveBayesModel.predict(lp.features))
		predictions.take(10).foreach(println)
		var predictionAndLabel = predictions.zip( ohe_test_rdd.map(_.label))
		var accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2 ).count/ohe_test_rdd.count
		println("Naive Bayes Model accuracy " + accuracy)
		
	}

	def parseCatFeatures(catfeatures: Array[String]) :  List[(Int, String)] = {
		var catfeatureList = new ListBuffer[(Int, String)]()
		for (i <- 0 until catfeatures.length){
		  	catfeatureList += i -> catfeatures(i).toString
		}
		catfeatureList.toList
	}
}