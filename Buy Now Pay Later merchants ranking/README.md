# Generic Buy Now, Pay Later Project

## Group 45

### Team Members:
- Aobo Li
- Jialiang Shen
- Jiqiang Chen
- Junkai Zhang
- Ying Zhu

### Research Goal:
The research goal is to assist the Buy Now, Pay Later (BNPL) company in identifying the best merchants to partner with by forecasting the expected revenue of each merchant in the following year.

### Instructions:
To run this project, follow these steps:

1. **Download Data:** Execute the following command in the terminal to run the `download.py` script.

    ```bash
    python3 ../generic-buy-now-pay-later-project-group-45/scripts/download.py --path ../generic-buy-now-pay-later-project-group-45/data/tables
    ```

2. **Preprocess Data:** Execute the following command in the terminal to run the `preprocess.py` script.

    ```bash
    python3 ../generic-buy-now-pay-later-project-group-45/scripts/preprocess.py --path ../generic-buy-now-pay-later-project-group-45/data/tables --output ../generic-buy-now-pay-later-project-group-45/data/curated
    ```

3. **Geospatial Plot:** Run `geoplot.ipynb` to produce the geospatial plot.

4. **Rank Merchants:** Execute the following command in the terminal to run the `ranking.py` script. Replace `dddd-dd-dd` with the most recent annual transaction data start date (e.g., `2021-08-28`).

    ```bash
    python3 ../generic-buy-now-pay-later-project-group-45/scripts/ranking.py --path ../generic-buy-now-pay-later-project-group-45/data/curated dddd-dd-dd
    ```

5. **Visualize Results:** Run `plots.ipynb` to produce plots used in the presentation and visualization of the final ranked merchants.

    > **Note:** Adjust the directory path `".."` in the above commands to match your local repository.

### Contact:
For further information or inquiries, feel free to email me at [aoboli2001@hotmail.com](mailto:aoboli2001@hotmail.com).





##
Overall Score(Individual's score): 54.5/60 
