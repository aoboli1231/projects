# MAST30034 Project 1

**Student Details:**
- **Name:** Aobo Li

## Research Objective

This research aims to analyze and compare the trip costs associated with Yellow Taxi and Uber services to determine a more cost-effective choice for potential riders in New York City.

## Study Duration

The project is based on data spanning from February 2019 to September 2019.

## Execution Instructions

### Data Acquisition and Preparation

Navigate to the `scripts` directory and execute the following scripts sequentially:

1. `download_data.py`: Fetches raw trip data, segregating and storing it in distinct folders for Yellow Taxi and Uber within the `data/raw` directory.
2. `download_sport_data.py`: Initiates a 'sport' folder and retrieves NYC's 2019 Sports events data, storing it under `data/raw/sport`.
3. `download_weather.py`: Forms a 'weather' folder and acquires NYC's 2019 Weather data, placing it in `data/raw/weather`.
4. `sort_sport_data.py`: Organizes the sports data chronologically, determines event start and end times, evaluates intervals when events take place, and saves this data alongside the sports information.

### Data Analysis and Visualization

Head to the `notebook` directory and execute the following Jupyter notebooks in sequence:

5. `project_1_preprocessing.ipynb`: Processes raw data and associates it with the Sports and Weather data. The refined datasets for Yellow Taxi (referenced as ytaxi) and Uber (denoted as HVFH) can be found in the `data/curated` directory.
6. `P1_Visualisation.ipynb`: Generates visualizations derived from the trip data. All visual plots are archived in the `plot` directory.
7. `P1_model.ipynb`: Constructs models for both Uber and Yellow Taxi based on selected features, runs predictions on test sets, and creates visualizations for these predictions. Resultant plots are housed in the `plot` directory.

## Evaluation

Final Score: **19.5/30**

**Feedback:** The project, although methodically executed, fell short in terms of its research complexity. Direct cost estimations for these services are readily available on respective official websites, diminishing the novelty of this investigation.
