import streamlit as st
import pandas as pd
import plotly.express as px
import dask.dataframe as dd

# --- Page Configuration ---
st.set_page_config(
    page_title="SCADA Data Analysis Tool",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Helper Functions ---

@st.cache_data
def load_data(uploaded_file):
    """
    Load data from the uploaded file. This version is updated for the new format:
    'Turbine Number,Variable,Date,Time,Value'
    """
    if uploaded_file is not None:
        try:
            # Read Excel file using pandas and convert to Dask DataFrame
            pandas_df = pd.read_excel(uploaded_file, engine='openpyxl')
            
            # Convert to Dask DataFrame for consistent processing
            dask_df = dd.from_pandas(pandas_df, npartitions=1)
            
            # --- Data Cleaning and Transformation ---
            # 1. Combine Date and Time into a single Timestamp column
            dask_df['Timestamp'] = dd.to_datetime(dask_df['Date'].astype(str) + ' ' + dask_df['Time'].astype(str))
            
            # 2. Create a unique, descriptive parameter name from Turbine and Variable
            dask_df['Parameter'] = dask_df['Turbine Number'].astype(str) + ' - ' + dask_df['Variable'].astype(str)
            
            # 3. Convert Value column to numeric, coercing errors to NaN
            dask_df['Value'] = dd.to_numeric(dask_df['Value'], errors='coerce')
            
            # 4. Drop rows with parsing errors in the Value column
            dask_df = dask_df.dropna(subset=['Value'])
            
            # 5. Select only the columns we need for the app
            dask_df = dask_df[['Timestamp', 'Parameter', 'Value']]
            
            return dask_df
        except Exception as e:
            st.error(f"Error processing file: {e}")
            st.warning("Please ensure the file has 'Turbine Number,Variable,Date,Time,Value' columns.")
            return None
    return None

def convert_df_to_csv(df):
    """
    Convert a Pandas DataFrame to a CSV string for downloading.
    """
    return df.to_csv(index=False).encode('utf-8')

# --- Main Application UI ---

st.title("📈 Interactive SCADA Data Visualization Tool")
st.markdown("""
**Final Version:** This tool is now configured for Excel files and creates colorful, SCADA-style charts.

**How to use:**
1.  **Upload your Excel file** on the left.
2.  The app automatically combines Turbine and Variable names.
3.  Use the sidebar to **filter by date and time**.
4.  **Select the parameters** you wish to plot.
5.  The interactive, colorful chart will update instantly.
""")

# --- Sidebar for User Inputs ---
with st.sidebar:
    st.header("⚙️ Controls")

    # 1. File Uploader
    st.subheader("1. Upload SCADA Excel File")
    uploaded_file = st.file_uploader(
        "Choose an Excel file",
        type="xlsx",
        help="Upload your SCADA data in Excel format."
    )

    # Initialize session state
    if 'dask_df' not in st.session_state:
        st.session_state.dask_df = None
    if 'filtered_df' not in st.session_state:
        st.session_state.filtered_df = pd.DataFrame()

    if uploaded_file is not None:
        with st.spinner('Loading and processing file...'):
            st.session_state.dask_df = load_data(uploaded_file)

    if st.session_state.dask_df is not None:
        dask_df = st.session_state.dask_df
        
        try:
            with st.spinner('Analyzing data range and parameters...'):
                parameter_options, min_date, max_date = dd.compute(
                    dask_df['Parameter'].unique(),
                    dask_df['Timestamp'].min(),
                    dask_df['Timestamp'].max()
                )

            # 2. Date & Time Range Selector
            st.subheader("2. Select Date & Time Range")
            date_range = st.date_input(
                "Filter by date",
                value=(min_date.date(), max_date.date()),
                min_value=min_date.date(),
                max_value=max_date.date()
            )
            
            start_time = st.time_input("Start time", value=min_date.time())
            end_time = st.time_input("End time", value=max_date.time())

            # Handle both single date and date range cases
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
            else:
                start_date = end_date = date_range

            start_datetime = pd.to_datetime(f"{start_date} {start_time}")
            end_datetime = pd.to_datetime(f"{end_date} {end_time}")

            # 3. Parameter Selection
            st.subheader("3. Select Parameters to Plot")
            selected_parameters = st.multiselect(
                "Choose parameters",
                options=sorted(parameter_options),
                default=list(parameter_options[:4]) if len(parameter_options) > 3 else list(parameter_options),
            )
            
            # Filter the Dask dataframe based on user selections
            filtered_dask_df = dask_df[
                (dask_df['Timestamp'] >= start_datetime) & 
                (dask_df['Timestamp'] <= end_datetime) &
                (dask_df['Parameter'].isin(selected_parameters))
            ]

        except Exception as e:
            st.error(f"An error occurred during data processing: {e}")
            selected_parameters = []
            filtered_dask_df = None
    else:
        st.info("Awaiting Excel file upload.")
        selected_parameters = []
        filtered_dask_df = None

# --- Main Panel for Chart and Data ---
if filtered_dask_df is not None and selected_parameters:
    with st.spinner("Generating colorful plot..."):
        plot_df = filtered_dask_df.compute()
        st.session_state.filtered_df = plot_df

        st.header("📊 SCADA Trend Chart")

        if not plot_df.empty:
            # Create two columns - chart on left, data panel on right
            chart_col, data_col = st.columns([9, 1])
            
            with chart_col:
                # Create an interactive line chart with a vibrant color sequence
                fig = px.line(
                    plot_df,
                    x='Timestamp',
                    y='Value',
                    color='Parameter',
                    title='Time Series Data for Selected Parameters - Click on chart to view values',
                    labels={'Value': 'Measurement', 'Timestamp': 'Date and Time'},
                    template='plotly_dark',
                    color_discrete_sequence=px.colors.qualitative.Vivid # Use a colorful palette
                )

                # Customize the plot for a professional SCADA look with massive height increase
                fig.update_layout(
                    legend_title_text='Parameters',
                    xaxis=dict(rangeslider=dict(visible=True), type="date"),
                    hovermode='x unified',
                    height=800,  # 10x increase for much better vertical analysis
                    margin=dict(l=20, r=20, t=20, b=40),  # Much larger margins for better spacing
                    showlegend=True,
                    yaxis=dict(range=[0, 500]),  # Focus on 0-500 range where data is concentrated
                    hoverlabel=dict(
                        bgcolor="rgba(0,0,0,0)",  # Fully transparent background
                        font=dict(color="black", family="Arial Black", size=12),  # Bold black text
                        bordercolor="rgba(0,0,0,0)",  # Transparent border
                        align="left"
                    )
                )
                
                # Custom hover template for clean, professional appearance
                fig.update_traces(
                    mode='lines', 
                    line=dict(width=2),
                    hovertemplate='<b>%{y}</b><extra></extra>'  # Only show bold value, remove trace name box
                )

                # Display the chart
                st.plotly_chart(fig, use_container_width=True)
            
            with data_col:
                st.subheader("📋 Data Inspector")
                st.markdown("Select a time point to view detailed values")
                
                # Add time picker for data inspection
                available_times = sorted(plot_df['Timestamp'].unique())
                
                if available_times:
                    # Default to the latest time
                    default_time = available_times[-1]
                    
                    selected_time = st.selectbox(
                        "Select time point:",
                        options=available_times,
                        index=len(available_times)-1,
                        format_func=lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
                    )
                    
                    clicked_time = selected_time
                    
                    # Get all parameters at the selected timestamp
                    timestamp_data = plot_df[plot_df['Timestamp'] == clicked_time]
                    
                    if not timestamp_data.empty:
                        st.markdown(f"**Time:** {clicked_time.strftime('%Y-%m-%d %H:%M:%S')}")
                        st.markdown("---")
                        
                        # Display each parameter value
                        for _, row in timestamp_data.iterrows():
                            param_name = row['Parameter']
                            value = row['Value']
                            
                            # Create a clean parameter display name
                            display_name = param_name.split(' - ')[-1] if ' - ' in param_name else param_name
                            
                            st.metric(
                                label=display_name,
                                value=f"{value:.2f}",
                                help=f"Full parameter: {param_name}"
                            )
                    else:
                        st.info("No data found for selected time")
                else:
                    st.info("No data available for inspection")

            # --- Data Download Section ---
            st.header("📥 Download Filtered Data")
            st.markdown("Download the data for the selected time range and parameters.")
            
            csv_data = convert_df_to_csv(st.session_state.filtered_df)

            st.download_button(
                label="Download data as CSV",
                data=csv_data,
                file_name=f"filtered_scada_data.csv",
                mime="text/csv",
            )
        else:
            st.warning("No data available for the selected time range and parameters.")
elif st.session_state.dask_df is not None:
    st.warning("Please select at least one parameter to plot.")
