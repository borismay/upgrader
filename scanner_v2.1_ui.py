#!/usr/bin/python
from siklu_api import *

import streamlit as st
import base64




##############################################################################
##############################################################################
if __name__ == '__main__':
    # scan
    # copy
    # run
    # accept

    st.title('Siklu command line automation engine')

    RINGS = int(st.number_input('Number of rings', value=3, format='%d'))
    MH_ENABLED = st.checkbox('Enable MH', True)
    N_PROCESSES = int(st.number_input('Number of parallel processes', value=10, format='%d'))
    CONNECTION_TIMEOUT_SEC = int(st.number_input('Connection timeout [sec]', value=12, format='%d'))

    CSV_FILENAME = st.file_uploader("Choose a CSV file", type="csv")

    if CSV_FILENAME is not None:
        hosts = pd.read_csv(CSV_FILENAME, comment='#')
        hosts.dropna(subset=['ip', 'user', 'command'], how='any', inplace=True)
        st.dataframe(hosts)

    if st.button('Run'):
        if CSV_FILENAME is not None:
            results_filename = units_manager_parallel(hosts)

            b64 = base64.b64encode(open(results_filename,
                                        'r').read().encode()).decode()  # some strings <-> bytes conversions necessary here
            href = f'<a href="data:file/csv;base64,{b64}" download="{results_filename}">Download CSV File</a>'
            st.markdown(href, unsafe_allow_html=True)
        else:
            print('No file specified')
