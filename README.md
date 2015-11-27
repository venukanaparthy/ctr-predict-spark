# ctr-predict-spark
Click stream prediction using spark


To run the code:
1. Build
sbt package

2. Run
..\spark-submit --class CTRPredict target\scala-2.10\ctrpredict_2.10-0.1.jar data\sample.csv
