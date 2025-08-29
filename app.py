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
def load_excel_data(uploaded_file):
    """
    Load data from the uploaded Excel file. This requires 'openpyxl'.
    """
    if uploaded_file is None:
        return None
    try:
        # Pandas is used here as Dask's read_excel is less direct.
        # For large files, this might be slow, but it's the most reliable way.
        pandas_df = pd.read_excel(uploaded_file, engine='openpyxl')
        
        # Convert to Dask DataFrame for consistent, memory-efficient processing later
        dask_df = dd.from_pandas(pandas_df, npartitions=2) # Use at least 2 partitions
        
        # --- Data Cleaning and Transformation ---
        # Ensure correct data types before combining
        dask_df['Date'] = dask_df['Date'].astype(str)
        dask_df['Time'] = dask_df['Time'].astype(str)
        dask_df['Turbine Number'] = dask_df['Turbine Number'].astype(str)
        dask_df['Variable'] = dask_df['Variable'].astype(str)
        
        dask_df['Timestamp'] = dd.to_datetime(dask_df['Date'] + ' ' + dask_df['Time'], errors='coerce')
        dask_df['Parameter'] = dask_df['Turbine Number'] + ' - ' + dask_df['Variable']
        dask_df['Value'] = dd.to_numeric(dask_df['Value'], errors='coerce')
        
        # Drop rows with any parsing errors
        dask_df = dask_df.dropna(subset=['Value', 'Timestamp'])
        
        return dask_df[['Timestamp', 'Parameter', 'Value']]

    except Exception as e:
        st.error(f"Error processing file: {e}")
        st.warning("Ensure the file is a valid Excel file with 'Turbine Number,Variable,Date,Time,Value' columns.")
        return None

def convert_df_to_csv(df):
    """Convert a Pandas DataFrame to a CSV string for downloading."""
    return df.to_csv(index=False).encode('utf-8')

# --- Main Application UI ---
st.title("SCADA Trend Data Visualization Tool")
st.markdown("Upload your raw **Excel file**. The app will automatically process and visualize the data.")

# --- Sidebar Controls ---
with st.sidebar:
    st.header("⚙️ Controls")
    uploaded_file = st.file_uploader("1. Upload SCADA Excel File", type=["xlsx", "xls"])

    if 'dask_df' not in st.session_state:
        st.session_state.dask_df = None

    if uploaded_file:
        with st.spinner('Loading and processing Excel file...'):
            st.session_state.dask_df = load_excel_data(uploaded_file)

    if st.session_state.dask_df is not None:
        dask_df = st.session_state.dask_df
        try:
            with st.spinner('Analyzing data range...'):
                parameter_options, min_date, max_date = dd.compute(
                    dask_df['Parameter'].unique(),
                    dask_df['Timestamp'].min(),
                    dask_df['Timestamp'].max()
                )

            st.subheader("2. Select Date & Time Range")
            date_range = st.date_input("Filter by date", value=(min_date.date(), max_date.date()), min_value=min_date.date(), max_value=max_date.date())
            start_time = st.time_input("Start time", value=min_date.time())
            end_time = st.time_input("End time", value=max_date.time())
            start_datetime = pd.to_datetime(f"{date_range[0]} {start_time}")
            end_datetime = pd.to_datetime(f"{date_range[1]} {end_time}")

            st.subheader("3. Select Parameters to Plot")
            selected_parameters = st.multiselect("Choose parameters", options=sorted(parameter_options), default=list(parameter_options[:4]) if len(parameter_options) > 3 else list(parameter_options))
            
            filtered_dask_df = dask_df[
                (dask_df['Timestamp'] >= start_datetime) & 
                (dask_df['Timestamp'] <= end_datetime) &
                (dask_df['Parameter'].isin(selected_parameters))
            ]
        except Exception as e:
            st.error(f"An error occurred: {e}")
            selected_parameters = []
            filtered_dask_df = None
    else:
        st.info("Awaiting Excel file upload.")
        selected_parameters = []
        filtered_dask_df = None

# --- Main Panel for Chart ---
if filtered_dask_df is not None and selected_parameters:
    with st.spinner("Generating plot..."):
        plot_df = filtered_dask_df.compute()

        with st.sidebar:
            st.subheader("4. Y-Axis Controls (Vertical Zoom)")
            if not plot_df.empty:
                default_min, default_max = float(plot_df['Value'].min()), float(plot_df['Value'].max())
                buffer = (default_max - default_min) * 0.05
                y_min_suggested = default_min - buffer if buffer > 0 else default_min - 1
                y_max_suggested = default_max + buffer if buffer > 0 else default_max + 1
                y_min = st.number_input("Y-Axis Min", value=y_min_suggested, format="%.2f")
                y_max = st.number_input("Y-Axis Max", value=y_max_suggested, format="%.2f")
            else:
                y_min, y_max = 0, 100
                st.info("No data for Y-axis range.")

        st.header("📊 SCADA Trend Chart")
        if not plot_df.empty:
            fig = px.line(
                plot_df, x='Timestamp', y='Value', color='Parameter',
                title='Time Series Data for Selected Parameters',
                template='plotly_dark', color_discrete_sequence=px.colors.qualitative.Vivid
            )
            fig.update_layout(
                yaxis_range=[y_min, y_max],
                legend_title_text='Parameters',
                xaxis=dict(rangeslider=dict(visible=True), type="date"),
                hovermode='x unified',
                hoverlabel=dict(
                    bgcolor="rgba(0, 0, 0, 0.7)",
                    font_size=14,
                    font_family="Arial"
                )
            )
            fig.update_traces(
                hovertemplate='<b>%{y:.2f}</b><extra></extra>'
            )
            st.plotly_chart(fig, use_container_width=True)

            st.header("📥 Download Filtered Data")
            st.download_button(
                "Download data as CSV", convert_df_to_csv(plot_df),
                "filtered_scada_data.csv", "text/csv"
            )
        else:
            st.warning("No data available for the selected time range and parameters.")
elif st.session_state.dask_df is not None:
    st.warning("Please select at least one parameter to plot.")
