//<<<<<<<<<<------- Census Data Analysis ----------->>>>>>>>>>>

import org.apache.spark.sql.{Column, Row, SQLContext, SparkSession}  //Explanation is already given in Assignment18.1

object Session20Assign1 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session20Assign2")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 20/Assignments/Assignment1")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  //setting path of winutils.exe
  System.setProperty("hadoop.home.dir", "F:/Softwares/winutils")
  //winutils.exe needs to be present inside HADOOP_HOME directory, else below error is returned:
  //error: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries

  //<<<<<<<--------- selection of few columns from census.csv file --------->>>>>>>>

  //reading data from census.csv file
  val census_data = spark.sparkContext.textFile("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 20/Assignments/Assignment1/census.csv")
  //census_data -->> RDD[String]

  //splitting data with delimiter ','
  val census_data1 = census_data.map(x => x.split(","))
  //census_data1 -->> RDD[Array[String]]

  //creating tuple of selected fields
  val census_data2 = census_data1.map(x => (x(0), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22)))
  //census_data2 -->> RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String)]

  //to convert rdd to dataframe, below import is required
  import spark.implicits._
  val census_data2_DF = census_data2.toDF("State", "Persons", "Males", "Females", "Growth_1991_2001", "Rural", "Urban", "Scheduled_Caste_population", "Percentage_SC_to_total", "Number_of_households", "Household_size_per_household", "Sex_ratio_females_per_1000_males ", "Sex_ratio_0_6_years", "Scheduled_Tribe_population", "Percentage_to_total_population_ST", "Persons_literate", "Males_Literate", "Females_Literate", "Persons_literacy_rate", "Males_Literatacy_Rate", "Females_Literacy_Rate", "Total_Educated")
  //census_data2_DF -->> sql.DataFrame

  //temporary view of census_data2_DF is created
  census_data2_DF.createOrReplaceTempView("census")
  //spark.sql("select * from census").show()
  //REFER Screenshot 1 for output

  //Below five queries will now work on this "census" view
  //********************************************************************************************

  //1. Find out the state wise population and order by state

  val population = spark.sql("select state,sum(persons) as total_population from census" +
    " group by state order by total_population desc")
  //population -->> sql.DataFrame
  population.show()
  //REFER Screenshot 2 for output

  //********************************************************************************************

  //2. Find out the Growth Rate of Each State Between 1991-2001

  val growth_rate = spark.sql("select state,avg(Growth_1991_2001) as total_growth from census group by state")
  //growth_rate -->>  sql.DataFrame
  growth_rate.show()
  //REFER Screenshot 3 for output

  //********************************************************************************************

  //3. Find the literacy rate of each state

  val literacy = spark.sql("select state,avg(Persons_literacy_rate) as literacy_rate from census group by state")
  //literacy -->> sql.DataFrame
  literacy.show()
  //REFER Screenshot 4 for output

  //********************************************************************************************

  //4. Find out the States with More Female Population

  val female_pop = spark.sql("select state, sum(Males)-sum(Females) as more_female_population from census group by state")
  //female_pop -->> sql.DataFrame
  female_pop.show()
  //REFER Screenshot 5 for output

  //********************************************************************************************

  //5. Find out the Percentage of Population in Every State

  val percent_pop = spark.sql("select state, (sum(persons) * 100.0) / SUM(sum(persons)) over() as percent_pop_by_state from census" +
    " group by state")
  //percent_pop -->> sql.DataFrame
  percent_pop.show()
  //REFER Screenshot 6 for output
  //Explanation of above query
  //1. A function is used as a window function by adding an OVER clause after a supported function in SQL, e.g. avg(revenue) OVER (...);
  //2. if over() from above query is omitted then below error is returned
  //org.apache.spark.sql.AnalysisException: It is not allowed to use an aggregate function in the argument of another aggregate function.
  // Please use the inner aggregate function in a sub-query.;

}