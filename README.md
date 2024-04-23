# IS3107 Group 2
# Property Price Analysis
IS3107 Project for creating a end-to-end data pipeline and downstream applications to support property price analysis in Singapore.

# Team Members
CHU JIA-CHENG, CHUANG KUANG YU, MARTIN, NG XIANG HAN, SEAH JIA JUN, ZHANG YU

# Objectives
1. Visualise recent property data based on location and proximity to nearby public facilities.
2. Visualise property prices over time for time trend analysis
3. Investigate the correlation between proximity to public amenities and property price. 
4. Predict prices of future similar properties given a set of property features using machine learning model(s)


# Data Engineering Pipeline
## ER Diagram
![image](https://github.com/magiciansz/is3107-property-price-analysis/assets/77622894/76991676-b05d-481a-97b6-a55bcb986e39)

## ETL Process
Our ETL process is split into the initial and update ETL for the purposes of backdating historical data and populating future data respectively. A basic DAG of the ETL process is shown below:
![image](https://github.com/magiciansz/is3107-property-price-analysis/assets/77622894/9d824e43-b3e2-44cb-854a-a25ab72ac2be)


# Downstream Application 1: Properties Data Visualisation
## Dashboard 1: Property Price
Visualises fluctuations in property prices over the years, based on input factors such as location, room type etc

![image](https://github.com/magiciansz/is3107-property-price-analysis/assets/77622894/6b016a53-c851-46a8-a127-74337b9751c1)

## Dashboard 2: District-Level Price Trend
A high-level view of property prices for home buyers to understand price trends in a district, and for urban planners to assess need for intervention (i.e. cooling measures)

![image](https://github.com/magiciansz/is3107-property-price-analysis/assets/77622894/59856cc8-fec2-414b-9951-e38546f95e78)

## Dashboard 3: Displaying of Projects and Amenities
A visualisation of amenities in a district, allowing home buyers to choose properties according to lifestyle needs

![image](https://github.com/magiciansz/is3107-property-price-analysis/assets/77622894/7eddb759-795c-498d-8942-d369eeb1bb67)

# Downstream Application 2: Price Prediction Regression Model
Summary of Results
|       | Before Hyperparameter Tuning |        |        | After Hyperparameter Tuning |        |        |
|-------|:----------------------------:|:------:|:------:|:---------------------------:|:------:|:------:|
|       | OLS                          | Lasso  | Ridge  | OLS                         | Lasso  | Ridge  |
| Train | 0.8507                       | 0.8239 | 0.8507 | -                           | 0.8494 | 0.8504 |
|  Test | 0.8537                       | 0.8264 | 0.8537 | -                           | 0.8530 | 0.8540 |

