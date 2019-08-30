# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Cmd + Shift + B'
#   Check Package:             'Cmd + Shift + E'
#   Test Package:              'Cmd + Shift + T'
options(encoding = "UTF-8")
library(ids)
library(jsonlite)

phOperatorFactory <- "com.pharbers.ipaas.data.driver.api.factory.PhOperatorFactory"
distinctOperator <- "com.pharbers.ipaas.data.driver.operators.DistinctOperator"
WithColumnRenamedOperator <- "com.pharbers.ipaas.data.driver.operators.WithColumnRenamedOperator"
SelectOperator <- "com.pharbers.ipaas.data.driver.operators.SelectOperator"
AddColumnOperator <- "com.pharbers.ipaas.data.driver.operators.AddColumnOperator"
PhBaseJob <- "com.pharbers.ipaas.data.driver.api.job.PhBaseJob"
PhJobFactory <- "com.pharbers.ipaas.data.driver.api.factory.PhJobFactory"
PhBaseAction <- "com.pharbers.ipaas.data.driver.api.job.PhBaseAction"
PhActionFactory <- "com.pharbers.ipaas.data.driver.api.factory.PhActionFactory"
ReadParquetOperator <- "com.pharbers.ipaas.data.driver.operators.ReadParquetOperator"
SelectOperator <- "com.pharbers.ipaas.data.driver.operators.SelectOperator"
UnionOperator <- "com.pharbers.ipaas.data.driver.operators.UnionOperator"
GroupOperator <- "com.pharbers.ipaas.data.driver.operators.GroupOperator"
JoinOperator <- "com.pharbers.ipaas.data.driver.operators.JoinOperator"
ExprFilterOperator <- "com.pharbers.ipaas.data.driver.operators.ExprFilterOperator"
DropOperator <- "com.pharbers.ipaas.data.driver.operators.DropOperator"
SaveParquetOperator <- "com.pharbers.ipaas.data.driver.operators.SaveParquetOperator"
AddDiffColsOperator <- "com.pharbers.ipaas.data.driver.operators.AddDiffColsOperator"
#plugin
PhPluginFactory <- "com.pharbers.ipaas.data.driver.api.factory.PhPluginFactory"
ExprPlugin <- "com.pharbers.ipaas.data.driver.plugins.ExprPlugin"

save(distinctOperator, WithColumnRenamedOperator,SelectOperator,AddColumnOperator, ReadParquetOperator, SelectOperator, UnionOperator, GroupOperator, JoinOperator, ExprFilterOperator,
     DropOperator, SaveParquetOperator, file = "./data/operatorReference.RData")
save(phOperatorFactory, file = "./data/operatorFactory.RData")
save(PhBaseJob, file = "./data/jobReference.RData")
save(PhJobFactory, file = "./data/jobFactory.RData")
save(PhBaseAction, file = "./data/actionReference.RData")
save(PhActionFactory, file = "./data/actionFactory.RData")
save(PhPluginFactory, file = "./data/pluginFactory.RData")
save(ExprPlugin, file = "./data/pluginFactory.RData")

#plugins
phSparklyr.exprPlugin <- function(expr) {
  load("./data/pluginFactory.RData")
  load("./data/pluginFactory.RData")
  exprPlugin <- list(name= uuid(1,TRUE), reference= ExprPlugin, factory= PhPluginFactory,args=list(exprString= expr))
  return(exprPlugin)
}

#Operators
phSparklyr.addDiffcol <- function(lst, moreColDF) {
  load("./data/operatorReference.RData")
  load("./data/operatorFactory.RData")
  inDFName <- lst[[length(lst)]][["name"]]
  moreColDFName <- moreColDF[[length(moreColDF)]][["name"]]
  operator <- list(name= uuid(1,TRUE), reference=AddDiffColsOperator, factory=phOperatorFactory, args=list(inDFName=inDFName, moreColDFName=moreColDFName))
  return(phSparklyr.asAction(c(lst, moreColDF), operator))
}

phSparklyr.saveParquet <- function(lst, path) {
  load("./data/operatorReference.RData")
  load("./data/operatorFactory.RData")
  inDFName <- lst[[length(lst)]][["name"]]
  operator <- list(name= uuid(1,TRUE), reference=SaveParquetOperator, factory=phOperatorFactory, args=list(inDFName=inDFName, path=path))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.castColType <- function(lst, newColType, colNames) {
  for (colName in colNames) {
    exprString <- paste0("cast(", colName, " as ", newColType, ")")
    lst <- phSparklyr.mutate(lst, colName, exprString)
  }
  return(lst)
}

phSparklyr.dropClumns <- function(lst, ...) {
  load("./data/operatorReference.RData")
  load("./data/operatorFactory.RData")
  inDFName <- lst[[length(lst)]][["name"]]
  drops <- paste(..., sep = "#")
  operator <- list(name= uuid(1,TRUE), reference=DropOperator, factory=phOperatorFactory, args=list(inDFName=inDFName, drops=drops))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.colExprCalc <- function(lst, ...) {
  load("./data/operatorReference.RData")
  load("./data/operatorFactory.RData")
  inDFName <- lst[[length(lst)]][["name"]]
  selects <- paste(..., sep = "#")
  selectID <- uuid(1,TRUE)
  selectOperator <- list(name= selectID, reference=SelectOperator, factory=phOperatorFactory, args=list(inDFName=inDFName, selects=selects))
  joinOperator <- list(name= uuid(1, TRUE), reference= JoinOperator, factory=phOperatorFactory, args=list(inDFName= inDFName, joinDFName= selectID, joinExpr= "TRUE", joinType= "full"))
  phSparklyr.asAction(lst, selectOperator, joinOperator)
}

phSparklyr.filter <- function(lst, filterExpr) {
  load("./data/operatorFactory.RData")
  load("./data/operatorReference.RData")
  df <- lst[[length(lst)]][["name"]]
  operator <- list(name= uuid(1, TRUE), reference= ExprFilterOperator, factory=phOperatorFactory, args=list(inDFName= df, filter= filterExpr))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.join <- function(lst, joinDF, joinExpr, joinType) {
  load("./data/operatorFactory.RData")
  load("./data/operatorReference.RData")
  df <- lst[[length(lst)]][["name"]]
  joinDFName <- joinDF[[length(joinDF)]][["name"]]
  operator <- list(name= uuid(1, TRUE), reference= JoinOperator, factory=phOperatorFactory, args=list(inDFName= df, joinDFName= joinDFName, joinExpr= joinExpr, joinType= joinType))
  return(phSparklyr.asAction(c(lst, joinDF), operator))
}

phSparklyr.group <- function(lst, groupCols, aggExprs) {
  load("./data/operatorFactory.RData")
  load("./data/operatorReference.RData")
  df <- lst[[length(lst)]][["name"]]
  aggExprs <- paste(aggExprs, collapse = "#")
  groups <- paste(groupCols, collapse = "#")
  operator <- list(name= uuid(1, TRUE), reference= GroupOperator, factory=phOperatorFactory, args=list(inDFName= df, groups= groups, aggExprs= aggExprs))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.distinct <- function(lst) {
  load("./data/operatorFactory.RData")
  load("./data/operatorReference.RData")
  df <- lst[[length(lst)]][["name"]]
  operator <- list(name= uuid(1,TRUE), reference=distinctOperator, factory=phOperatorFactory, args=list(inDFName=df))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.rename <- function(lst, oldColName= "", newColName= "") {
  load("./data/operatorFactory.RData")
  load("./data/operatorReference.RData")
  df <-  lst[[length(lst)]][["name"]]
  operator <- list(name= uuid(1,TRUE), reference=WithColumnRenamedOperator, factory=phOperatorFactory, args=list(inDFName=df, oldColName=oldColName, newColName=newColName))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.mutate <- function(lst, newColName= "", plugin= "") {
  load("./data/operatorFactory.RData")
  load("./data/operatorReference.RData")
  inDFName <- lst[[length(lst)]][["name"]]
  operator <- list(name= uuid(1,TRUE), reference=AddColumnOperator, factory=phOperatorFactory, args=list(inDFName=inDFName, newColName=newColName), plugin= phSparklyr.exprPlugin(plugin))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.select <- function(lst, ...) {
  load("./data/operatorReference.RData")
  load("./data/operatorFactory.RData")
  inDFName <- lst[[length(lst)]][["name"]]
  selects <- paste(..., sep = "#")
  operator <- list(name= uuid(1,TRUE), reference=SelectOperator, factory=phOperatorFactory, args=list(inDFName=inDFName, selects=selects))
  return(phSparklyr.asAction(lst, operator))
}

phSparklyr.bind <- function(lst, unionDFList){
  load("./data/operatorReference.RData")
  load("./data/operatorFactory.RData")
  inDFName <- lst[[length(lst)]][["name"]]
  unionDFName <- unionDFList[[length(unionDFList)]][["name"]]
  operator <- list(name= uuid(1,TRUE), reference=UnionOperator, factory=phOperatorFactory, args=list(inDFName=inDFName, unionDFName=unionDFName))
  return(phSparklyr.asAction(c(lst, unionDFList), operator))
}

phSparklyr.readParquet <- function(.path) {
  load("./data/operatorReference.RData")
  load("./data/operatorFactory.RData")
  load("./data/actionReference.RData")
  load("./data/actionFactory.RData")
  operator <- list(name= uuid(1,TRUE), factory= phOperatorFactory, reference= ReadParquetOperator, args= list(path= .path))
  action <- list(name= uuid(1,TRUE), reference= PhBaseAction, factory= PhActionFactory, args=list(null="null"), opers=list(operator))
  return(list(action))
}

#Operator asAction
phSparklyr.asAction <- function(lst, ...) {
  load("./data/actionReference.RData")
  load("./data/actionFactory.RData")
  action <- list(name= uuid(1,TRUE), reference= PhBaseAction, factory= PhActionFactory, args=list(null="null"), opers=list(...))
  lst[[length(lst)+1]] <- action
  return(lst)
}

# run
phSparklyr.execute <- function(...) {
  load("./data/jobFactory.RData")
  load("./data/jobReference.RData")
  actions <- unique(c(...))
  return(list(list(name=uuid(1,TRUE), reference=PhBaseJob, factory=PhJobFactory, actions=actions)))
  #return(toJSON(list(name=uuid(1,TRUE), reference=PhBaseJob, factory=PhJobFactory, actions=actions), auto_unbox = TRUE))
}


phSparklyr.writeJson <- function(lst, .path) {
  jsonResult <- toJSON(lst, auto_unbox = TRUE)
  write(prettify(jsonResult), .path)
}




