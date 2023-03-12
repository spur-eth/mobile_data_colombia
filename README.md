## Project: GPS location data from mobile phones to study impacts of Bogota cable car opening

#### Collaboration with Elena Lutz in the SPUR group

### Some background: 
A flat part of Bogota city and some poorer neighborhoods on hills in the south that are now connected to the city center by a cable car that was recently built and opened in 2019. We want to study the impact of this cable car opening on the movements of people who live in that neighborhood compared to a similar area of the city that continues to lack a public transit connection to the city center. 

### Data:
Providers (private companies) collect mobile phone data at a large scale that is available for purchase. We are considering buring the data and focusing on the relevant areas within Bogota (have to buy data for all of Colombia and filter to those pings from within the city).
- Currently have two months worth of data for testing (have data from month after the opening of the cable car 2019, also a month from 2022)

### Goal: Do analysis to see if the month of data is good enough to be worth buying 2018 data (more expensive) and data from 2019 to study the impact of the cable car
- How to know if it is good enough (2 key factors): How many observations appear in Bogota and what is their granularity and usefulness for tracking individual movements (e.g. can we see locations/movements between different neighborhoods?)

### Tasks: 
1) CALCULATE TOTAL NUMBER OF OBSERVATIONS IN BOGOTA
    - each observation is a ping, timestamped, individual identifiers for each device
2) REGULARITY OF OBSERVATIONS PER DEVICE OVER TIME
    - How long do I observe this device over the month? How often do I see each person per day?

### Notes from 2023-03-08: 
- Drop users seen for less than 10 days and with less than 60 pings. But also want to be able to determine the home locations of the people. For the home determination, try the the following options for "bed time" (home time):
    1. 10pm to 6am 
    2. 11pm-5am

#### Next steps:
- Number of pings during hours when home locations could be computed (per user). How many people? (see above for cutoffs)
- Map of home locations, especially see the regions in the south if we have some users from there. Generate a heatmap. Want to see some granularity.
- lower priority: Plots of user loss as the cutoffs for pings and days are varied.

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


### Additional resources: 
