Sys.setenv(SPARK_HOME="/Users/alfredyang/Desktop/spark/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="/Users/alfredyang/Desktop/hadoop-3.0.3/etc/hadoop/")

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(BPCalSession)
library(BPRDataLoading)

ss <- BPCalSession::GetOrCreateSparkSession("TMCal", "client")
df <- createDataFrame(
           list(list(1L, 7, "7"), list(2L, 2, "8"), list(3L, 3, "9")),
           c("a", "b", "c"))
schema <- structType(structField("a", "integer"), structField("b", "double"),
                     structField("c", "string"), structField("d", "integer"))

df1 <- dapplyCollect(
                df,
                function(x) {
                    print(x)
                    x[x[1] == 1, ]
                }
            )

print(head(df1))