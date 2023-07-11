## Project: GPS location data from mobile phones to study impacts of Bogota cable car opening

#### Collaboration with Elena Lutz in the SPUR group

### Some background: 
A flat part of Bogota city and some poorer neighborhoods on hills in the south that are now connected to the city center by a cable car that was recently built and opened in 2019. We want to study the impact of this cable car opening on the movements of people who live in that neighborhood compared to a similar area of the city that continues to lack a public transit connection to the city center. 

### Data:
Providers (private companies) collect mobile phone data at a large scale that is available for purchase. We are considering buring the data and focusing on the relevant areas within Bogota (have to buy data for all of Colombia and filter to those pings from within the city).
- Currently have two months worth of data for testing (have data from month after the opening of the cable car 2019, also a month from 2022)

### Goal 1: Do analysis to see if the month of data is good enough to be worth buying 2018 data (more expensive) and data from 2019 to study the impact of the cable car
- How to know if it is good enough (2 key factors): How many observations appear in Bogota and what is their granularity and usefulness for tracking individual movements (e.g. can we see locations/movements between different neighborhoods?)

### Goal 2: Map all the stratum, home and work locations for all the people living in Bogota that pass quality control to be able to compare this information across neighborhoods and station locations. 

### Goal 3: Map the nearest POIs of all the pings to be able to match specific buildings to the ping locations 


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

### Additional resources: 
