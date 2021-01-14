# Data Engineering Project Notebook

## Group: Data Analyzers

    1. Islam Nasr.
    2. Gaser Khaled.
    3. Kariman Hossam.
    4. Marwan Karim.
    
This repository contains a notebook that demonstates our data engineering process for three datasets, which are:  
    1. [World Happiness Report](https://www.kaggle.com/unsdsn/world-happiness)  
    2. [All 250 Country Data](https://www.kaggle.com/souhardyachakraborty/all-250-country-data)  
    3. [Life Expectancy (WHO)](https://www.kaggle.com/kumarajarshi/life-expectancy-who)  
Moreover, We also integrated with an external dataset in order to get accurate Population figures for the available countries from [here](https://population.un.org/wpp/Download/Standard/Population/).  

## Milestone 1:

    1. We imported the 3 datasets.
    2. We explored the datasets.
    3. We performed the needed cleaning and tidying to the datasets.
    4. We dropped the unwanted or unreliable data from the datasets.
    5. We did some data imputation in order to fill in the needed misssing data.
    6. We merged the datasets together into one big dataset which was then used to answer our research questions.
    7. Answering our research questions, which are:
        a. What is the relationship between life expectency and happiness in each region?
        b. What is the happiness factor that contributes the most to the happiness score, based on the Region?
        c. What is the count of countries with life expectancy less/greater than the total average in each year?
        d. Number of deaths by Measles in each region.
        e. What is the ranking of Egypt based on the happiness score over the years?
        
## Milestone 2:

### Milestone 2A:
    
    1. Feature Engineering. We added 3 new features to the datasets after we merged them (these features are attributes that can be inferred from the already given attributes). These are:
        a. Calculating the population density of each country in each year.
        b. Calculating the growth rate of life expectancy for each country every year.
        c. Calculating the population growth rate for each country every year.
    2. We then visualised these features and explored their added values. We also recorded our insights observed from these features.
   
### Milestone 2B:
    
    1. Creating a simplified ETL pipeline using Airflow.  
        a. Extracting the data from a CSV file. Each dataset is read and sent as a pd.DataFrame to the next phase.
        b. Transforming the data by working on it (cleaning and tidying).   
        c. Loading the data into new CSV files. These files are saved onto disk with new names.


This was achieved by creating a **DAG.py** file, which can be found on the repository. This file is supposed to be placed in an airflowhome directory, inside the **dag** folder, from which an Airflow pipeline would be initiated. Moreover, there is another file that needs to be placed inside the same **dag** folder, which is the **ML.py** file. This file is called by the **DAG.py** file in order to run a machine learning model (linear regression) in order to predict values in one of the datasets. This file is ran as a *BashOperator* so it needs to be present in the same directory.   

This directory (airflowhome) must also include a **data** directory which should hold all the needed CSV files in the extract phase (the 3 datasets mentioned earlier).  

The DAG is then initiated from the Airflow terminal.  

Upon completion, the **data** directory would contain 5 new CSV files named:   
    
    1. Country Dataset NEW  
    2. Happiness Dataset NEW  
    3. Life Expectancy NEW  
    4. Merged Datasets  
    5. Research Question 1 Answer  

The two CSV files named "Merged Datasets" and "Research Question 1 Answer" are the CSV files of interest. They have the complete, merged datasets as well as the dataframe that would be used to answer our first research question respectively.  

## Milestone 3
In this milestone we were supposed to create a create a pipeline using Airflow. This pipeline which was scheduled to run daily for 4 days (we also ran it for 2 extra days) and performed the following steps: 

1. Fetching 50 tweets from 2 different countries, a country that we categorized as unhappy (Egypt) and a country that was categorized as happy (Canada). This was found out using the previous analysis in milestone 2.
2. We performed some text cleaning. We disregarded retweets, removed links and emojis.
3. We performed sentiment analysis (polarity and subjectivity) on the tweets using python's [Textblob library.](https://textblob.readthedocs.io/en/dev/)
4. We then calculated the average of these sentiments for each day and was outputted to a file storing the average sentiments for each country. We also compared this average with the happiness score of the country in 2019 (using the happiness dataset).
5. An extra step was performed on the happiness dataset happiness score. The values of polarity are from -1 to 1. The happiness score from the happiness dataset and the happiness (polarity) from the tweets are inconsistent. Therefore, we normalized the happiness scores in the happiness dataset for the year 2019 using the [MinMaxScaler](https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MinMaxScaler.html) from -1 to 1. This enabled us to do a more realistic comparison of the happiness averages. 

**Dag file flow:**  
The dag file is split into 3 separate bash operators that run 3 different python scripts.
1. Extracting data from the twitter API using [tweepy.](https://www.tweepy.org/). This is done through the *Extract*.py file.
2. Cleaning and preforming sentiment analysis. Done through *Transform*.py file.
3. Calculating the average and comparing it to the happiness dataset. Done through *Average*.py file.

### Requirements:
Some requirements are needed in order to be able to run the Twitter DAG file. The *Twitter_DAG.py* file can be found on the repository. This file is supposed to be placed in an airflowhome directory, inside the **dags** folder, from which an Airflow pipeline would be initiated. Moreover, the three files listed above (*Extract*.py,  *Transform*.py and *Average*.py) should also be placed inside the **dags** folder. Finally, the CSV file outputs from the previous pipeline in Milestone 2B should also be available inside the **data** folder.

Using the above scripts (bash operators) will require extra python packages to be installed onto the VM. These scripts run in the terminal of the VM using the command **"python3 SCRIPT_NAME"**, where SCRIPT_NAME is to be replaced with one of these **.py** files. These packages are:
			
	1. pip3
	2. python3
	3. tweepy
	4. textblob
	5. pandas
	6. re
	7. nltk
	8. sklearn.preprocessing
<n>

Moreover, a *TwitterConfig*.py file is also needed to be included in the **dags** folder. It will include this *twitter* object, which has the needed keys extracted from the respective user's twitter development account. This file has not been added to the repository in order to preserve the confidentiality of these keys.

	twitter = {
		'consumer_key': '',
		'consumer_secret': '',
		'key': '',
		'secret': ''
		}

### Output
Inside the data folder uploaded, there is the output .CSV files from running the Airflow pipeline for 5 days.
The CSV files that have the tweets have columns: 
			
	1. Country (the name of the country from which the tweet is)
	2. Screen_name (the user name that wrote the tweets)
	3. Tweet (the tweet text file after cleaning)
	4. Polarity
	5. Subjectivity

The CSV file that has the Tweets Averages has these columns:
		
	1. Country
	2. Date (time stamp in which the average was calculated)
	3. Average_Polarity (for the day)
	4. Average_Subjectivity (for the day)
	5. Comparison_To_Happiness_Score (whether the day average is greater or less than the Happiness_Score)
