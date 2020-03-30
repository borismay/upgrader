import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import glob
import os.path
import pandas as pd
import datetime
import random
import streamlit as st
import plotly_express as px
import time

import plotly
import plotly.figure_factory as ff
from plotly.graph_objs import *
from plotly.tools import make_subplots

from statsmodels.tsa.seasonal import seasonal_decompose
from db_wrapper import *
from stats_downloader import RF_STATS_TABLE_NAME, ETH_STATS_TABLE_NAME

@st.cache(allow_output_mutation=True)
def load_data(filename):
    data = pd.read_csv(filename)
    data.index = pd.to_datetime(data['start_ts'])
    return data[data['host'].notna()].sort_index(ascending=True)


@st.cache(allow_output_mutation=True)
def load_data_database(table='rf'):
    db = levitan_db()
    db_engine = db.engine
    db_name = db.db_name
    if table == 'rf':
        sql_query = r"select * from %s.%s ;" % (db_name, RF_STATS_TABLE_NAME)
    else:
        sql_query = r"select * from %s.%s ;" % (db_name, ETH_STATS_TABLE_NAME)

    data = pd.read_sql(sql_query, db_engine).drop_duplicates()
    data.index = pd.to_datetime(data['start_ts'])
    return data[data['host'].notna()].sort_index(ascending=True)


def plot_rssi(df_, ips):
    fig = Figure()
    for ip in ips:
        df = df_[df_['host'] == ip]
        avg_rssi = df['max-rssi'] - (df['max-rssi'] - df['min-rssi']) / 2.0

        fig.add_trace(Scatter(
            x=df.index,
            y=df['max-rssi'],
            name="max-rssi",
            showlegend=False,
            line=dict(color="#33CFA5"),
        ))

        fig.add_trace(Scatter(
            x=df.index,
            y=df['min-rssi'],
            name="min-rssi",
            showlegend=False,
            line=dict(color="#F06A6A"),
        ))

        fig.add_trace(Scatter(
            x=df.index,
            y=avg_rssi,
            name="avg-rssi, %s" % ip,
            line=dict(color="#000000"),
        ))

    fig.layout.title = 'RSSI'
    return fig

def plot_rssi_delta(df_, ips, reference = 0, show_plot = False):
    fig = Figure()

    if show_plot:
        visible = True
    else:
        visible = 'legendonly'
    for ip in ips:
        df = df_[df_['host'] == ip]
        delta_rssi = df['max-rssi'] - df['min-rssi']

        fig.add_trace(Scatter(
            x=df.index,
            y=delta_rssi,
            name="%s" % ip,
            visible=visible,
        ))

    x = [df_.index.min(), df_.index.max()]
    y = [reference, reference]
    fig.add_trace(Scatter(
        x=x,
        y=y,
        name="delta: %d" % reference,
        line=dict(color="rgba(255, 0, 0, 0.5)", dash='dash'),
    ))

    fig.layout.title = 'RSSI Delta'
    return fig

def plot_rssi_stats(df_, ip):
    df = df_[df_['host'] == ip]
    delta_rssi = df['max-rssi'] - df['min-rssi']
    hist_data = [df['max-rssi'], df['min-rssi'], delta_rssi]
    group_labels = ['max-rssi', 'min-rssi', 'delta-rssi']
    return ff.create_distplot(hist_data, group_labels)

def plot_rssi_decomp(df_, ip, period):
    df = df_[df_['host'] == ip]
    delta_rssi = (df['max-rssi'] + df['min-rssi']) / 2
    s = seasonal_decompose(delta_rssi, model='additive', period=period)
    x = df.index
    fig = make_subplots(rows=4, cols=1)

    fig.append_trace(Scatter(
        x=s.observed.index,
        y=s.observed,
        showlegend=False,
    ), row=1, col=1)
    fig.update_yaxes(title_text="Observed", row=1, col=1)

    fig.append_trace(Scatter(
        x=s.trend.index,
        y=s.trend,
        showlegend=False,
    ), row=2, col=1)
    fig.update_yaxes(title_text="Trend", row=2, col=1)

    fig.append_trace(Scatter(
        x=s.seasonal.index,
        y=s.seasonal,
        showlegend=False,
    ), row=3, col=1)
    fig.update_yaxes(title_text="Seasonal", row=3, col=1)

    fig.append_trace(Scatter(
        x=s.resid.index,
        y=s.resid,
        showlegend=False,
    ), row=4, col=1)
    fig.update_yaxes(title_text="Residual", row=4, col=1)

    return fig

################################################################
def main():
    st.title('Siklu Smart Suite')

    # data_filename = st.sidebar.text_input('Stats filename:', r'c:\Python\upgrader\rf_stats_w.csv')
    # df = load_data(data_filename)
    df = load_data_database()

    ips = df['host'].drop_duplicates().to_list()
    selected_ips = st.sidebar.multiselect('Chose IPs', ips, default=[])

    rssi_delta_db_ref = st.sidebar.slider('RSSI delta dB', 2, 20, value=4, step=2)
    period = int(st.sidebar.text_input('Decom period:', 96))

    if st.sidebar.button('Run RF analysis'):
        data = df[df['host'].isin(selected_ips)]
        st.write('From: {} to {}'.format(data.index.min(), data.index.max()))
        st.dataframe(data.describe())
        # RSSI
        st.header('RSSI')
        st.plotly_chart(plot_rssi(df, selected_ips))
        st.plotly_chart(plot_rssi_delta(df, selected_ips, reference=rssi_delta_db_ref, show_plot=True))

        for selected_ip in selected_ips:
            st.header('RSSI statistics: {}'.format(selected_ip))
            st.plotly_chart(plot_rssi_stats(df, selected_ip))
            st.header('RSSI decomposition: {}'.format(selected_ip))
            st.plotly_chart(plot_rssi_decomp(df, selected_ip, period))

        # CINR

if __name__ == '__main__':
    main()