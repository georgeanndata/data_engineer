
# Not a one trick pony, completing and improving the process: How I used Kafka, Spark, Postgres, and Metabase in a Docker container to create a data engineering platform


# Introduction & Goals
Last year I completed a [data science project]
(https://georgeanndata.github.io/2021/08/01/insurance_fraud_detection.html) where I used Python, Pandas and Tableau to evaluate and report on auto insurance fraud. After completing the project, I wondered if it would be easier for a data scientist to access the data without having to import the csv file themself and how much better it would be for them to be working on the most updated data available, instead of a file they had stored locally, which in all probability was not current.  Since I have an ETL and data quality background, I decided to take another program to learn data engineering so that I could create a robust data engineering platform that would conveniently supply the most updated auto insurance fraud data to the data scientist, thus not only making their jobs easier but also improving their analysis and reporting.  

Intrestink links:
*My data science project can be found here:  https://georgeanndata.github.io/2021/08/01/insurance_fraud_detection.html
* Data Science Infinity
* Data Engineering Academy

In this project I have taken the auto claims data I used in my data science project
- Introduce your project to the reader
- Orient this section on the Table of contents
- Write this like an executive summary
  - With what data are you working
  - What tools are you using
  - What are you doing with these tools
  - Once you are finished add the conclusion here as well

  Architecture
  ![Architecture](images/insurance_project_architecture_.png)


# Contents

- [The Data Set](#the-data-set)
- [Used Tools](#used-tools)
  - [Connect](#connect)
  - [Buffer](#buffer)
  - [Processing](#processing)
  - [Storage](#storage)
  - [Visualization](#visualization)
- [Pipelines](#pipelines)
  - [Stream Processing](#stream-processing)
    - [Storing Data Stream](#storing-data-stream)
    - [Processing Data Stream](#processing-data-stream)
  - [Batch Processing](#batch-processing)
  - [Visualizations](#visualizations)
- [Demo](#demo)
- [Conclusion](#conclusion)
- [Follow Me On](#follow-me-on)
- [Appendix](#appendix)


# The Data Set
- Explain the data set
- Why did you choose it?
- What do you like about it?
- What is problematic?
- What do you want to do with it?

# Used Tools
- Explain which tools do you use and why
- How do they work (don't go too deep into details, but add links)
- Why did you choose them
- How did you set them up

## Connect
## Buffer
Apache Kafka
## Processing
Apache Spark
## Storage
PostGres
## Visualization
Metabase

# Pipelines
- Explain the pipelines for processing that you are building
- Go through your development and add your source code

## Stream Processing
### Storing Data Stream
### Processing Data Stream
## Batch Processing
## Visualizations

# Demo
- You could add a demo video here
- Or link to your presentation video of the project

# Conclusion
Write a comprehensive conclusion.
- How did this project turn out
- What major things have you learned
- What were the biggest challenges

# Follow Me On
Add the link to your LinkedIn Profile

# Appendix

[Markdown Cheat Sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)
