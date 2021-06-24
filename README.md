# WeatherData_Analysis

Spark SQL을 활용하여 날씨 데이터를 비교그래프로 나타내어 보는 것을 목표로한다. 

> https://0equal2.tistory.com/160?category=478380

  

0. Spark 실행

1. DataFrame으로 CSV파일 읽어오기

2. 날짜와 평균 기온 추출

3. 비교그래프(1) - **꺾은선 그래프 그리기**

   3-0. 월별 평균 기온 출력

   3-1. Pandas 형태 변환

   3-2. 꺾은선 그래프 비교

4. 비교그래프(2) - **막대 그래프 그리기**

   4-0. 월별 평균 기온 출력

   4-1. Pandas 형태 변환

   4-2. 막대 그래프 비교

---



- 2011~2020년의 날씨데이터를 공공데이터포털에서 수집
- 일 단위로 서울의 평균기온, 일강수량, 평균풍속, 평균상대습도, 평균기압, 평균최심신적설, 평균전운량을 수집

![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2FRkkad%2Fbtq4J7VGV5D%2F4bBNVp19a8Ncasq3j6ecQk%2Fimg.png)



- Table Structure

  ![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2Fp0qmC%2Fbtq4IDOjFl8%2FT4winhOJfBEngg4qqDzty1%2Fimg.png)

> data.kma.go.kr/data/grnd/selectAsosRltmList.do?pgmNo=36



# 0. Spark 실행

```python
import findspark findspark.init() import pyspark from pyspark import SparkConf from pyspark import SparkContext from pyspark.sql import SQLContext from pyspark.sql import SparkSession sc = SparkSession.builder\ .master('local[*]')\ .appName('hello_world_app')\ .getOrCreate()
```



# 1. DataFrame으로 CSV파일 읽어오기

```python
#1. 데이터 DataFrame으로 읽기
#1-1. dataframe 스키마
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

schema=StructType([\
    StructField("local",IntegerType()),\
    StructField("local_name",StringType()),\
    StructField("date",StringType()),\
    StructField("ave_temperature",DoubleType()),\
    StructField("daily_precipitation",DoubleType()),\
    StructField("ave_windspeed",DoubleType()),\
    StructField("ave_relative_humidity",DoubleType()),\
    StructField("ave_airpressure",DoubleType()),\
    StructField("the_latest_snowfall",DoubleType()),\
    StructField("ave_total_cloud",DoubleType())])
#1-2. 파일 불러오기
PATH='weather_20110101_20201231.csv'
file_df=sc.read.csv(PATH, header=True,schema=schema)
print(file_df)
#1-3. 데이터 확인
file_df.show()
```

![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2Fca15e6%2Fbtq4MDTPLfG%2FHSgg6Hjef6PHS31IV0EGr0%2Fimg.png)

```python
#1-4. 데이터 스키마 확인
file_df.printSchema()
```

![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2Fc0rYr3%2Fbtq4MBoa5VP%2FjjIOlF5PXrOVOaA6fYyI1K%2Fimg.png)



# 2. 날짜와 평균 기온열 추출

```python
#2-1. 기온 열값만 뽑아내기
date_temp_df=file_df['date','ave_temperature']
date_temp_df.show()
```

![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2FciRVt1%2Fbtq4Jw80gRj%2FfHTVMh1sx9kTsADstW62x0%2Fimg.png)

```python
#2-2. 우선 2011년도 월별 평균기온 구하기 
from pyspark.sql.functions import * 
temp_2011_df=date_temp_df.select(col('date'),col('ave_temperature'))\ 
	.where(year('date')=='2011')\
    .select(date_format('date','MM').alias('date'),col('ave_temperature'))\
    .groupBy('date').avg()\ 
    .orderBy('date') 

temp_2011_df.show() 
temp_2011_df.count() #temp_2011_df.printSchema()
```

- **.select :** 선택 column
- **.where :** 조건 ('date' column에서 year(년도)가 '2011'인 것)
- **.select(date_format('date','MM').alias('date'),col('ave_temperature'))\ :** 날짜를 월 형식으로 변환하고 열 이름을 'date'로 변환
- **.groupBy('date').avg() :** 'date'가 같은것끼리(같은 월별끼리) 평균기온 구하기
- **.orderBy('date') :** 'date'(월별)순으로 정렬

![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2FbKWoUb%2Fbtq4MUnxiio%2Fw9KSusOW7KAiKpeDxkunBk%2Fimg.png)

# 3. 비교그래프(1) - 꺾은선 그래프 그리기

우선 2011년도 데이터만 출력

```python
#3.선그래프 그리기 
from matplotlib import pyplot as plt

temp_2011_pd=temp_2011_df.toPandas()

x_values=temp_2011_pd['date']
y_values=temp_2011_pd['avg(ave_temperature)']

plt.figure(figsize=(30, 10))
plt.plot(x_values,y_values,color='green',marker='o')

plt.xlabel('month')
plt.ylabel('ave_temperature')
plt.legend(['2011'])
plt.show()category=478380 
```

![](https://img1.daumcdn.net/thumb/R1280x0/?scode=mtistory2&fname=https%3A%2F%2Fblog.kakaocdn.net%2Fdn%2FK4huL%2Fbtq4J8tyaK3%2FKr8iMZsCAeZqhFaXJYwkkK%2Fimg.png)

## 3-0. 월별 평균 기온 출력

```python
#3-0. 년도별로 월별 평균 기온 출력 
range_year=[i for i in range(2011,2021)] 
temp_year=['temp_'+str(i)+'_df' for i in range_year] 
print(range_year) 
print(temp_year) 
for i in range(len(temp_year)): 
    temp_year[i]=date_temp_df.select(col('date'),col('ave_temperature'))\ 
    .where(year('date')==str(range_year[i]))\
    .select(date_format('date','MM').alias('date'),col('ave_temperature'))\ 
    .groupBy('date').avg()\ 
    .orderBy('date') 
	#temp_year[i].show()
```

## 3-1. Pandas 형태 변환

그래프를 그리기 위해서 Spark DataFrame을 Pandas형태로 변환

```python
#3-1. 그래프를 그리기 위해 pd로 변환 
temp_year_pd=['temp_'+str(i)+'_pd' for i in range_year] 

for i in range(len(temp_year_pd)): 
    temp_year_pd[i]=temp_year[i].toPandas() 
    
display(temp_year_pd[0])
```

## 3-2. 꺾은선 그래프 비교

2018~2020년(3년)간의 꺾은선 그래프 비교

```python
#3-2. 우선 3년간 선그래프 비교 (2018-2020) 

#축 사이즈 plt.figure(figsize=(30, 10)) 

#그래프 그리기 
x_values=temp_year_pd[7]['date'] 
y_values=temp_year_pd[7]['avg(ave_temperature)'] 
plt.plot(x_values,y_values,marker='o') 

x_values=temp_year_pd[8]['date'] 
y_values=temp_year_pd[8]['avg(ave_temperature)'] 
plt.plot(x_values,y_values,marker='o') 

x_values=temp_year_pd[9]['date'] 
y_values=temp_year_pd[9]['avg(ave_temperature)'] 
plt.plot(x_values,y_values,marker='o') 

#축 라벨 표시 
plt.xlabel('month') 
plt.ylabel('ave_temperature') 
plt.legend(['2018','2019','2020']) 

plt.show()
```

![img](https://blog.kakaocdn.net/dn/c0AEf4/btq4HvwxWw8/Uz6GDJtgC3kLlmj478KXPk/img.png)

# 4. 비교그래프(2) - 막대 그래프 그리기

## 4-0. 월별 평균 기온 출력 (3-0.과 동일)

## 4-1. Pandas 형태 변환 (3-1.과 동일)

## 4-2. 막대 그래프 비교

2018~2020년(3년)간의 막대 그래프 비교

```python
#4-2. 우선 3년간 막대그래프 비교 (2018-2020)
import numpy

#축 사이즈
plt.figure(figsize=(30, 10))

#그래프 그리기
X=numpy.arange(12)
print(X)
_value=temp_year_pd[7]['avg(ave_temperature)']
plt.bar(X-0.25+1,_value,width=0.25)

_value=temp_year_pd[8]['avg(ave_temperature)']
plt.bar(X+0.00+1,_value,width=0.25)

_value=temp_year_pd[9]['avg(ave_temperature)']
plt.bar(X+0.25+1,_value,width=0.25)


#축 눈금 표시
ax = plt.subplot()
ax.set_xticks([i for i in range(1,13)])
#축 라벨 표시
plt.xlabel('month')
plt.ylabel('ave_temperature')
plt.legend(['2018','2019','2020'])


plt.show()
```

![img](https://blog.kakaocdn.net/dn/0vUJF/btq4MCglHgx/7ujps5VmMgkxwSviTvFzKK/img.png)