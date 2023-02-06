# MAST30034 Project 1 README.md
- Name: Aobo Li
- Student ID: 1172339


**Research Goal:** My research goal is to compare trip cost between Yellow Taxi and Uber service, choose the cheaper service if a customer needs to travel in New York City

**Timeline:** The timeline for the research area is Feb 2019 to Sep 2019

To run the pipeline, please visit the `scripts` directory and run the files in order:
1. `download_data.py`: This downloads the raw trip data into the `data/raw` directory, and create new folder for Yellow taxi and Uber data
2. `download_sport_data.py`: This create 'sport' folder for sports data and download New York City's 2019 Sport event data into the `data/raw/sport` directory 
3. `download_weather.py`: This create 'weather' folder for weather data and download New York City's 2019 Weather dat into the `data/raw/weather` directory
4. `sort_sport_data.py`: this sorts Sports data's datetime, create event's start time and end time, and calculate time periods where event occurs, and store this time interval in the same directory as sports data

Then visit the `notebook` directory and run the files in order:

5. `project_1_preprocessing.ipynb`: This notebook is used to preprocess raw data and join raw data with Sport and weather data. Filtered data will be stored in `data/curated` directory with ytaxi (yellow taxi) and HVFH(uber) folder
6. `P1_Visualisation.ipynb`: The notebook is used to create visualisation for the trip data, plots are stored in `plot` directory
7. `P1_model.ipynb`: The notebook is used to create model for Uber and Yellow taxi data with selected features, and predict on test data. This also includes visualisation for predictions, plots are stored in `plot` directory
