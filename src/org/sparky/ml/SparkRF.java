package org.sparky.ml;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.configuration.Strategy;
import org.apache.spark.mllib.tree.impurity.Gini;
import org.apache.spark.mllib.tree.impurity.Impurity;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import org.sparky.utils.Args;

import scala.Tuple2;

public class SparkRF {
	
	@SuppressWarnings({ "serial", "serial", "serial" })
	public static void main(String[] args){
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaRandomForestClassification");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// Load and parse general params
		String datapath = Args.parseString(args, "trainlibsvm", "");
		int minPartitions = Args.parseInteger(args, "minPartitions", 10);
		double trainFrac = Args.parseDouble(args, "trainFrac", 0.7);
		
		// Setting numFeatures to -1; will be determined from the data
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath,-1,minPartitions).toJavaRDD();
		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{trainFrac, 1-trainFrac});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];
		
		// Load and parse random forest params
		Integer numTrees = Args.parseInteger(args, "numTress", 100);
		Integer maxDepth = Args.parseInteger(args, "treeDepth", 5);

		// Train a RandomForest model.
		// Empty categoricalFeaturesInfo indicates all features are continuous.
		Integer numClasses = 2;
		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		String featureSubsetStrategy = "auto"; // Let the algorithm choose.
		Integer maxBins = 32;
		Integer seed = 12345;
		
		// Creating the impurity criteria
		Impurity imp = Gini.instance();
		
		Strategy strgy = new  Strategy(Algo.Classification(), imp, maxDepth, numClasses, maxBins, categoricalFeaturesInfo);
		strgy.setSubsamplingRate(1.0/numTrees);
		
		final RandomForestModel model = new RandomForest(strgy,numTrees, featureSubsetStrategy,seed).run(trainingData.rdd());
		
		// Since spark doesn't return predicted class probabilities, we will need to calculate them :( :(
		
		Broadcast<DecisionTreeModel[]> trees = sc.broadcast(model.trees());
		JavaRDD<Tuple2<Object,Object>> predictionAndLabel = testData.map(new Function<LabeledPoint, Tuple2<Object,Object>>(){

			@Override
			public Tuple2<Object, Object> call(LabeledPoint pt) throws Exception {
				Double prob=0.0;
				for(DecisionTreeModel tr : trees.value()){
					prob=prob+tr.predict(pt.features());
				}
				prob = prob/trees.value().length;

				return new Tuple2<Object, Object>(prob,pt.label());
			}

		});


		
		// Get evaluation metrics.
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabel.rdd(),100);
		
		
		// Precision by threshold
		JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
		System.out.println("Precision by threshold: " + precision.collect());
		
		// Recall by threshold
		JavaRDD<Tuple2<Object, Object>> recall = metrics.recallByThreshold().toJavaRDD();
		System.out.println("Recall by threshold: " + recall.collect());
		
		// F Score by threshold
		JavaRDD<Tuple2<Object, Object>> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
		System.out.println("F1 Score by threshold: " + f1Score.collect());
		JavaRDD<Tuple2<Object, Object>> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
		System.out.println("F2 Score by threshold: " + f2Score.collect());
		
		// Precision-recall curve
		JavaRDD<Tuple2<Object, Object>> prc = metrics.pr().toJavaRDD();
		System.out.println("Precision-recall curve: " + prc.collect());
		
		// Thresholds
		JavaRDD<Double> thresholds = precision.map(
		  new Function<Tuple2<Object, Object>, Double>() {
		    @Override
		    public Double call(Tuple2<Object, Object> t) {
		      return new Double(t._1().toString());
		    }
		  }
		);
		
		// ROC Curve
		JavaRDD<Tuple2<Object, Object>> roc = metrics.roc().toJavaRDD();
		System.out.println("ROC curve: " + roc.collect());

		// AUPRC
		System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());

		// AUROC
		System.out.println("Area under ROC = " + metrics.areaUnderROC());
		
		System.out.println("Learned classification forest model:\n" + model.toDebugString());

		String modelOutFile = Args.parseString(args, "modelOutName", "RFmod");
		
		// Save and load model
		model.save(sc.sc(), modelOutFile);
		//RandomForestModel sameModel = RandomForestModel.load(sc.sc(), "myModelPath");
		
		sc.close();

	}

}


