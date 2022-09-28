# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

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

### Matrix Profile End ###
