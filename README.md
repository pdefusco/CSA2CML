# CSA2CML Demo


This demo shows how you can create a real time dashboard in the Cloudera Data Platform. In order to execute it you need the following prerequisites:

* A Cloudera Machine Learning (CML) Workspace
* A Cloudera Streaming Analytics (CSA) DataHub with SQL Stream Builder
* CDP Data Lake Permissions including Ranger and proper ID Broker Mappings

Project Index

1. Deploy Streamlit AMP in CML
2. Create a streaming table in SQL Stream Builder
3. Modify your dashboard to consume real time data from SSB
4. Familiarize yourself with the Matrix Profile algorithm and embed it in the dashboard


## Part 1 - Deploy Streamlit AMP

From your CML Workspace, click on the AMPs tab on the left side panel

![alt text](docs/images/1_amp1.png)

A series of AMPs will be loaded in the middle of the screen. Select the Streamlit AMP as shown below. If you don’t see the Streamlit AMP you can load it into the AMP catalog following instructions (here).

![alt text](docs/images/2_amps2.png)

Click the “Configure Project” button. A window with deployment options will appear. You can leave default settings (default AMP settings are normally vetted by the Fast Forward Labs team who are in charge of AMPs).

If it is enabled, you can optionally disable the Spark option as it is not needed.

![alt text](docs/images/3_amps3.png)

Next, click on the “Launch Project” button at the bottom right. CML will automatically build the AMP project for you. This build should take 5-10 minutes at most.

Please feel free to stretch your legs and grab a coffee!

![alt text](docs/images/4_amp4.png)

You can now access the AMP by going back to the Projects homepage and opening the project. CML automatically names the project with the AMP title and your username, in my case “Streamlit - pauldefusco”.

![alt text](docs/images/5_project.png)

Notice the README file is automatically loaded for you. This contains useful information about the AMP.

Next, familiarize yourself with the Streamlit application. Open “Applications” on the left side panel and then on the “Streamlit App” box

![alt text](docs/images/6_streamlit_app1.png)

Your browser will open a new window with the Streamlit dashboard

![alt text](docs/images/7_streamlit_app2.png)

So how does this work? The actual code for the Streamlit dashboard is in the app.py file located in the project home folder. The CML Applications feature loads the script in a container and provides an endpoint for serving the app.

Notice that the base script used by the app is actually code/launch_app.py. The script references the code in app.py. Separating the code managing the deployment from the app is a CML best practice that allows you to better manage and version your apps in a CICD fashion, but it is not mandatory.

The data rendered by the dashboard is stored in the seaborn-data folder in the project home folder. Notice this is a small static csv file. The data is loaded in the application once and it does not mutate.

Author recommendation: feel free to click on the app.py script to see the application code, or the heyser.csv file located in the “seaborn=data” folder to inspect the data.

![alt text](docs/images/8_streamlit_app3.png)

Next, we will create a Flink table with SQL Stream Builder to create some more fake data for the CML Application. The goal is to have a dashboard showing insights in real time.


## Part 2 - Create a streaming table in SQL Stream Builder

Navigate to the Data Hub Clusters page from the CDP home page, then open the CSA cluster

![alt text](docs/images/9_datahub_flink.png)

And click on the Streaming SQL Console icon to open SQL Stream Builder

![alt text](docs/images/10_datahub_ssb.png)

SQL Stream Builder is a service to create queries on streams of data through SQL. It is part of Cloudera Streaming Analytics and is available both in Cloudera Private and Public Cloud.

Recommendation: for more information on this, please visit the following links:

* SSB Documentation
* SSB Short Intro video
* SSB Webinar

Next, we will create a Flink table from the SQL Console and populate it with fake data with Flink-Faker.

Please navigate to the Console -> SQL tab and select “Templates” -> “Faker”

![alt text](docs/images/11_sql_console_faker1.png)

To create the Flink table, replace the auto-populated SQL syntax with the following SQL DDL:

```
CREATE TEMPORARY TABLE geyser_ssb (
  `duration` DECIMAL,
  `waiting` INT,
  `kind` STRING,
  `timestamp` TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'fields.duration.expression' = '#{number.numberBetween ''1'',''5''}',
  'fields.waiting.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.kind.expression' = '#{regexify ''(long|short){1}''}',
  'fields.timestamp.expression' = '#{date.past ''2'',''SECONDS''}'
);
```

![alt text](docs/images/12_geyser_ddl.png)

Submit the Flink Job by clicking on the “Execute” button on the right hand side of the screen. Please validate that the table has been created by checking in the Logs tab below.

Note you have created a table with the same attributes that were present in the "geyser.csv" file we inspected in CML, plus a timestamp field.

Next, we will create a Materialized View. Replace the SQL syntax in the console with the following query. Do not submit the job.

```
SELECT * FROM geyser_ssb;  
```

Leave the syntax unchanged and open the “Materialized View” tab:

![alt text](docs/images/13_materialized_view1.png)

After a few seconds the Configuration tab will automatically detect the query and inspect potential primary keys.

You can ignore any initial messages. After a few seconds you will be able to change the "Materialized View" setting from "Disabled" to "Enabled" and select "timestamp" as the primary key.

The remaining options can be left to their default values.  

![alt text](docs/images/13_materialized_view1.png)

The next screen displays a set of configurations for the View. Please name the URL Pattern as shown with “SSB2CML” and make sure to “Select All” fields.

![alt text](docs/images/14_materialized_view2.png)

Save the configuration and patiently wait a moment for the URL Pattern tab to be populated with the endpoint at the bottom of the screen.

![alt text](docs/images/15_materialized_view3.png)

Copy the URL to your clipboard. We will use it in CML.

Next, do not navigate away from the console. Go back to the "SQL" tab and execute the job.

![alt text](docs/images/16_materialized_view4.png)

After a few moments the job will start and you will see live updates in the Results tab. Your Flink job is running and all with the ease of a SQL query!


## Part 3 - Modify your dashboard to consume real time data from SSB

Navigate back to the CML Workspace and open the CML Project where you deployed your dashboard i.e. “Streamlit - yourusername”.

Navigate to the Applications tab and remove the current application.

![alt text](docs/images/17_materialized_view5.png)

Next, navigate to the project home folder by clicking on “Overview” and open the requirements.txt file. Click on “Open in Workbench” at the top right to edit it.

![alt text](docs/images/18_remove_app.png)

Do not remove the current file contents. Rather, move the cursor to line 3 and add the following entries. Please make sure you enter each entry in a single line.

```
pandas
streamlit-autorefresh
numpy
requests-kerberos
```

![alt text](docs/images/19_modify_reqs1.png)

Launch a new CML session with "Workbench Editor". Open the terminal window and reinstall the requirements with submitting the following command:

```
pip3 install -r requirements.txt
```

![alt text](docs/images/20_modify_reqs2.png)

Next, navigate to the "Project Settings" tab on the left side panel and then open the "Advanced" tab. Create a new environment variable with the SSB Materialized View url you copied to the clipboard.

If you lost the URL value, you can navigate back to SSB and access it from the Materialized Views tab.

Name the variable "SSB_MV" by entering "SSB_MV" on the left side and paste the SSB url you copied to the clipboard on the right side as shown below.

![alt text](docs/images/21_run_job.png)

![alt text](docs/images/22_ssb_mv_envvar.png)

One last step: navigate back to the Overview tab and open the app.py file. Replace its contents with the following (a copy is stored in app.py in this repository)

```
import seaborn as sns
import streamlit as st
import pandas as pd
import json
import requests
from streamlit_autorefresh import st_autorefresh
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import os

st.title("Old Faithful eruptions")

a = os.environ["SSB_MV"].replace("gateway", "manager0").split("/")
ssb_endpoint = "https://"+a[2]+"/api/v1/query/"+a[-2]+"/"+a[-1]

#geyser = sns.load_dataset("geyser")
# update every 5 seconds
st_autorefresh(interval= 1 * 1000, key="dataframerefresh")

def load_data(endpoint):
  r = requests.get(endpoint, auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL))
  data = pd.DataFrame(json.loads(r.text))
  data[['duration','waiting']] = data[['duration','waiting']].astype('int')
  data["timestamp"] = pd.to_datetime(data["timestamp"])
  return data

geyser = load_data(ssb_endpoint)

st.markdown(
    """
    This is a tiny app to explore the
    [Old Faithful Geyser Data](https://www.stat.cmu.edu/~larry/all-of-statistics/=data/faithful.dat).
    First, we can view some summary statistics.
    The `duration` variable is the duration of an eruption in minutes,
    and the `waiting` variable is the time between eruptions in minutes.
    """
)

st.write(geyser.describe().T)

"""
So the mean waiting time between eruptions is around 70 minutes,
with a mean eruption duration of three and a half minutes.
Summary statistics can be misleading.
Let us plot the waiting and duration variables against each other.

We'll use a joint density plot, with the marginal densities for each
variable on the corresponding axis.
There are clearly two clusters:
shorter eruptions with a shorter waiting time,
and longer eruptions with a longer waiting time.
These are labeled in our data set, so we separate the data and color by cluster.
"""


with sns.axes_style("white"):
    st.pyplot(
        sns.jointplot(data=geyser, x="waiting", y="duration", hue="kind", kind="kde")
    )
```

You are now ready to deploy the new application with streaming data. Navigate to the Applications tab on the right side. Create a new application with the following settings:

Name: Streaming Dashboard
Subdomain: StreamingApps
Script: cml/launch_app.py
Resource Profile: at least 4vCPU / 8 GiB Memory recommended

No other values require modification. If you can’t find a resource profile with 4 vCPU / 8 GiB Memory, you can add it at the workspace level by navigating to “Site Administration” -> “Runtime/Engine” -> “Resource Profiles”. You will need Administrator rights to the workspace in order to do that.

![alt text](docs/images/23_final_app_settings.png)

Click create and then wait for the Application to launch. This might take up to a minute or two but shouldn’t take longer than that.

Notice that now both the table with summary statistics and the jointplot are updating in realtime. Streamlit has a built-in method named "st_autorefresh" that allows you to set update frequency. In "app.py" this has been added at line 57 and update frequency has been set at once per second.

Obviously this should be used carefully as we are effectively reloading the entire dataset (4 attributes x 300 rows ca.) into the container every second.  

![alt text](docs/images/24_final_app_screenshot.png)


## Part 4 -  Familiarize yourself with the Matrix Profile algorithm and embed it in the dashboard

As shown in part 3, go to the Applications tab in CML and stop the Streamlit application you created.

Edit the "requirements.txt" file by adding the "stumpy" library at line 7. Your "requirements.txt" file should include the following entries (make sure each is on a separate line):

```
seaborn==0.11.1
streamlit==0.78.0
pandas
streamlit-autorefresh
numpy
requests-kerberos
stumpy
```

Using the CML session with "Workbench Editor" you launched earlier, reopen the terminal window and reinstall the requirements with

```
pip3 install -r requirements.txt
```

Next, start a CML Session with the following mandatory settings:

```
Editor: JupyterLab
Kernel: Python 3.7
Edition: Standard
Resource Profile: 2 vCPU / 4 GiB Memory +
```

In your JupyterLab environment, open the "matrixProfileQuickstart.ipynb" notebook and run each cell by pressing "Shift + Enter" on your keyboard.

![alt text](docs/images/25_JN_screenshot.png)

The notebook is designed to be a crash course to the Matrix Profile and the Stumpy library. If you are curious to dive deeper, please visit any of the following resources:

* Intro to Matrix Profile in the Stumpy Docs
* Matrix Profile Foundation homepage
* A quick and to the point blog article

Finally, open the "app.py" file and replace all its contents with the following code. Notice you are using Stumpy at the bottom to render two diagrams showing insights provided by the Matrix Profile algorithm.

```
import seaborn as sns
import streamlit as st
import pandas as pd
import numpy as np
import json
import requests
from streamlit_autorefresh import st_autorefresh
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import os
import stumpy
import matplotlib.pyplot as plt

st.title("Old Faithful eruptions")

st.header('This dashboard is automatically updated every 3 seconds. You can customize update frequency.')

a = os.environ["SSB_MV"].replace("gateway", "manager0").split("/")
ssb_endpoint = "https://"+a[2]+"/api/v1/query/"+a[-2]+"/"+a[-1]

#geyser = sns.load_dataset("geyser")
# update every 5 seconds
st_autorefresh(interval= 3 * 1000, key="dataframerefresh")

def load_data(endpoint):
    r = requests.get(endpoint, auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL))
    data = pd.DataFrame(json.loads(r.text))
    data[['duration','waiting']] = data[['duration','waiting']].astype('int')
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data = data[:100]
    return data

geyser = load_data(ssb_endpoint)


st.markdown(
    """
    This is a tiny app to explore the
    [Old Faithful Geyser Data](https://www.stat.cmu.edu/~larry/all-of-statistics/=data/faithful.dat).
    First, we can view some summary statistics.
    The `duration` variable is the duration of an eruption in minutes,
    and the `waiting` variable is the time between eruptions in minutes.
    """
)

st.write(geyser.describe().T)

"""
So the mean waiting time between eruptions is around 70 minutes,
with a mean eruption duration of three and a half minutes.
Summary statistics can be misleading.
Let us plot the waiting and duration variables against each other.

We'll use a joint density plot, with the marginal densities for each
variable on the corresponding axis.
There are clearly two clusters:
shorter eruptions with a shorter waiting time,
and longer eruptions with a longer waiting time.
These are labeled in our data set, so we separate the data and color by cluster.
"""

with sns.axes_style("white"):
    st.pyplot(
        sns.jointplot(data=geyser, x="waiting", y="duration", hue="kind", kind="kde")
    )

### Matrix Profile Beginning ###

T_df = geyser['waiting']
Q_df = pd.Series([12,39,45,1,9,140])

distance_profile = stumpy.mass(Q_df.astype(np.float64), T_df.astype(np.float64))

idx = np.argmin(distance_profile)

st.header("The Matrix Profile algorithm scans the entire time series and identifies the subset that is most similar to the pattern provided")

"""
Q_df is the query pattern we provide to the algorithm. In this case it is the Pandas Series [12, 39, 45, 1, 9, 140]
T_df is the entire time series we are streaming from SQL Stream Builder (Flink).
"""

st.header(f"The nearest neighbor to Q_df is located at index {idx} in T_df")

Q_z_norm = stumpy.core.z_norm(Q_df.values)
nn_z_norm = stumpy.core.z_norm(T_df.values[idx:idx+len(Q_df)])

fig = plt.figure(figsize=(7,8))
plt.suptitle('Comparing The Query To Its Nearest Neighbor', fontsize='15')
plt.xlabel('Time', fontsize ='15')
plt.ylabel('Waiting', fontsize='15')
plt.plot(Q_z_norm, lw=2, color="C1", label="Query Subsequence, Q_df")
plt.plot(nn_z_norm, lw=2, label="Nearest Neighbor Subsequence From T_df")
plt.legend()
st.pyplot(fig)

st.header("The Matrix Profile algorithm scans the entire time series and identifies the subset that is most similar to the pattern provided")

fig = plt.figure(figsize=(7,8))
plt.suptitle('Geyser Dataset, T_df', fontsize='15')
plt.xlabel('Time', fontsize ='15')
plt.ylabel('Waiting', fontsize='15')
plt.plot(T_df)
ax = plt.gca()
plt.plot(range(idx, idx+len(Q_df)), T_df.values[idx:idx+len(Q_df)], lw=2, label="Nearest Neighbor Subsequence")
plt.legend()
st.pyplot(fig)
```

A copy of the final script is stored in this Github repository in the code folder.

You are now ready to redeploy the CML Application. In the CML Applications deployment form, please ensure to select "code/launch_app.py" as the script used for the app. The code is in "app.py" but the app is actually deployed via "launch_app.py".

A similar Resource Profile as shown in part 3 is recommended i.e. at least 4 vCPU / 8 GiB Memory +

![alt text](docs/images/26_streamlit_mp1.png)

![alt text](docs/images/27_streamlit_mp2.png)


## Next Steps

Matrix Profile is not the only algorithm you can use for Anomaly Detection with Flink:

* You can follow the steps outlined in [this article](https://community.cloudera.com/t5/Community-Articles/Flink-SSB-Credit-Card-Fraud-Detection-Demo/ta-p/334792) to flag anomalies directly in Flink based on geographic distance. Credits to Sunile Manjee, Principal Solutions Engineer at Cloudera.  
* You can try using the up and coming Flink based ML libraries. For example, you can prototype a Tensorflow or PyTorch model in CML and deploy it in Flink using [dl-on-flink](https://github.com/flink-extended/dl-on-flink)
