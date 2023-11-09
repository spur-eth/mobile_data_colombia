## Project: GPS location data from mobile phones to study impacts of Bogota cable car opening

#### Collaboration with Elena Lutz in the SPUR group

### Some background: 
A flat part of Bogota city and some poorer neighborhoods on hills in the south that are now connected to the city center by a cable car that was recently built and opened in 2019. We want to study the impact of this cable car opening on the movements of people who live in that neighborhood compared to a similar area of the city that continues to lack a public transit connection to the city center. 

### Data:
We obtained some data from providers (private companies) that collect mobile phone data at a large scale that is available for purchase in Colombia. This data is about 300 GB in size.

### Research question: What impacts does the opening of the cable car have on mobility of the people who live nearby? Does it open up new opportunties for them? 
For example for these users (compared to those living in control areas): 
- Do they make more trips? 
- Do they make longer trips? 
- Do they go to different types of places than control users?

## Setup 
1) Get data from AWS.

2) Create environment (requires [conda](https://docs.conda.io/en/latest/)) 

    ```shell
    conda create --name geo_mobile pip python=3.9
    conda activate geo_mobile
    pip install --upgrade pip
    pip install -r code/requirements.txt
    ```
    
## Run
1) To run the analysis, activate the geo_mobile environment as above, clone this repository and select the notebook that you want to run and click "Kernel" and then "Restart and run all." 

The main notebooks are numbered in order of execution and their basic functionality is: 

- `00_filter_data.ipynb`: filters the raw pings to a) only those within the study areas of Bogota specified by the shp file in the config and b) compute some user statistics and use this to select those pings from users with more than a minimum numbers of pings nd number of days detected.
- `01_find_filter_user_homes.pynb` : can be used to calculated the home and work location of each user and then record those whose home location is within a treatment or control ZAT
- `02_mobility_analysis.ipynb`: computes distance between home and work locations for each user and visits of different length 
- `03_visit_compute.ipynb`: maps the visits computed in the `mobility_analysis` notebook to POIs from Google
- `04_visit_analysis.ipynb`: generates graphs for some introductory overview of the temporal patterns of user visits detected over days and months for each year. 

### Additional resources: 
