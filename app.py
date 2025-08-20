import streamlit as st
import polars as pl
import pandas as pd
import io
import plotly.express as px
from pygwalker.api.streamlit import init_streamlit_comm, StreamlitRenderer
import streamlit.user_info

# Silence Streamlit deprecated user warning
streamlit.user_info.maybe_show_deprecated_user_warning = lambda: None


def validate_csv(uploaded_file, required_cols):
    """Validate uploaded CSV file and check required columns."""
    if not uploaded_file.name.lower().endswith(".csv"):
        raise ValueError(f"{uploaded_file.name} is not a valid .csv file.")

    df = pl.read_csv(uploaded_file).lazy()
    trimmed_names = {col: col.strip() for col in df.columns}
    df = df.rename(trimmed_names)


    col_names = [c.lower().strip() for c in df.collect_schema().names()]
    missing_cols = [col for col in required_cols if col.lower() not in col_names]
    if missing_cols:
        raise ValueError(f"❌ Missing required columns: {', '.join(missing_cols)}")

    return df


def process_tickets(daily_file, tickets_file):
    daily_tickets = validate_csv(daily_file, ['short_description', 'ALARMS'])
    tickets = validate_csv(tickets_file, ['Controlling Object Name', 'Alarm Time', 'Alarm Text'])

    daily_tickets = daily_tickets.with_columns(
        pl.col('short_description')
        .str.extract(r"\)\s*([A-Za-z0-9_]+)")
        .str.strip_chars()
        .alias('site_code')
    )

    spliced = tickets.select([
        pl.col('Controlling Object Name').str.strip_chars().alias('site_code'),
        pl.col('Alarm Time').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S').alias('START TIME'),
        pl.col('Alarm Text')
    ])

    grouped = (
        spliced.sort('START TIME', descending=True)
        .group_by('site_code')
        .agg([
            pl.col('Alarm Text').first(),
            pl.col('START TIME').first()
        ])
    )

    merged = daily_tickets.join(grouped, on='site_code', how='left')

    merged = merged.with_columns([
        pl.when(pl.col('Alarm Text').is_null())
        .then(pl.lit('NOT IN NMS'))
        .when(pl.col('Alarm Text').fill_null('').str.contains('NE3SWS AGENT NOT RESPONDING TO REQUESTS'))
        .then(pl.lit('VALID'))
        .otherwise(pl.lit('INVALID'))
        .alias('VALIDATION')
    ])

    merged = merged.with_columns([
        pl.when(pl.col('VALIDATION') == 'INVALID')
        .then(None)
        .otherwise(pl.col('START TIME'))
        .alias('START TIME'),
        pl.col('site_code').alias('SITE CODE')
    ])

    final_set = merged.select([
        'number', 'opened_at', 'short_description', 'sys_updated_on',
        'ALARMS', 'VALIDATION', 'START TIME', 'SITE CODE'
    ]).collect()

    return final_set.to_pandas()


def get_unique(df):
    df = pl.from_pandas(df)
    grouped = (
            df.lazy().group_by('SITE CODE')
            .agg(pl.len().alias('Alarm Count'))
            .sort('Alarm Count', descending=True)
    ).collect()
    return grouped.to_pandas()


def main():
    st.set_page_config(page_title="Ticket Validator", layout="wide")
    init_streamlit_comm()

    st.markdown("""
    <style>
    /* Center align headers and content for the tabular data */
    .stDataFrame [data-testid='stDataFrame'] table {
        width: 100%;
    }
    
    .stDataFrame [data-testid='stDataFrame'] th,
    .stDataFrame [data-testid='stDataFrame'] td {
        text-align: center !important;
    }
    
    /* Specifically target the SITE CODE and Alarm Count columns */
    div[data-testid="stDataFrame"] td:nth-child(2),
    div[data-testid="stDataFrame"] th:nth-child(2) {
        text-align: center !important;
    }
    
    div[data-testid="stDataFrame"] td:nth-child(3),
    div[data-testid="stDataFrame"] th:nth-child(3) {
        text-align: center !important;
    }
    
    /* Style the pie chart annotations */
    .pie-annotation {
        text-align: center;
        font-weight: bold;
    }
    
    /* Style the file uploader section */
    .stFileUploader {
        text-align: center;
    }
    
    /* Center the main title */
    h1 {
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)

    st.title("🎫 Ticket Validation Tool")
    st.markdown("Upload **Daily Tickets** and **Tickets** CSV files to process validation.")

    col1, col2 = st.columns(2)
    with col1:
        daily_file = st.file_uploader("📎 Upload Daily Tickets (.csv)", type=['csv'], key="daily")
    with col2:
        tickets_file = st.file_uploader("📎 Upload Tickets (.csv)", type=['csv'], key="tickets")

    if daily_file is None or tickets_file is None:
        st.info("Please upload **both** CSV files to proceed.")
        st.stop()

    if 'df_pandas' not in st.session_state:
        try:
            st.info("Processing data, please wait...")
            st.session_state.df_pandas = process_tickets(daily_file, tickets_file)
            st.success("✅ Data transformation complete!")
        except Exception as e:
            st.error(f"⚠️ An error occurred: {e}")
            st.stop()

    st.subheader("📌 Preview of Transformed Data")
    st.dataframe(st.session_state.df_pandas.head(20), use_container_width=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        st.session_state.df_pandas.to_excel(writer, index=False, sheet_name='Validated Tickets')
    output.seek(0)

    st.download_button(
        label="📥 Download as Excel",
        data=output,
        file_name="validated_tickets.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # PIE CHART & TABULAR
    df = st.session_state.df_pandas
    tab_pie, tab_tabular = st.tabs(["PIE CHART", "TABULAR"])

    if "VALIDATION" not in df.columns:
        with tab_pie:
            st.warning("No `VALIDATION` column found. Make sure the pipeline creates it first.")
        with tab_tabular:
            st.info("Tabular view coming soon.")
    else:
        order = ["VALID", "INVALID", "NOT IN NMS"]
        counts = (
            df["VALIDATION"].value_counts(dropna=False)
            .reindex(order)
            .fillna(0)
            .astype(int)
        )

        with tab_pie:
            
            dark_mode = st.toggle("🌙 Dark Mode", value=False)

            text_color = "white" if dark_mode else "black"
            bg_color = "black" if dark_mode else "white"
            grid_color = "#444" if dark_mode else "#ddd"

            order = ["VALID", "INVALID", "NOT IN NMS"]
            counts = (
                df["VALIDATION"].value_counts(dropna=False)
                .reindex(order)
                .fillna(0)
                .astype(int)
            )
            values = [int(counts.get(k, 0)) for k in order]

            fig = px.pie(
                names=order,
                values=values,
                hole=0,
                color=order,
                color_discrete_map={
                    "VALID": "green",
                    "INVALID": "orange",
                    "NOT IN NMS": "blue"
                }
            )

            fig.update_traces(
                textinfo="label+percent",
                textposition="inside",
                insidetextfont=dict(size=14, color="white"),
                hoverinfo="skip",
                hovertemplate=None
            )

            fig.update_layout(
                title=dict(
                    text="Validation Distribution",
                    x=0.5, y=0.95,
                    xanchor="center",
                    yanchor="top",
                    font=dict(size=30, color=text_color, family="Arial")
                ),
                autosize=True,
                height=560,
                margin=dict(t=140, b=40, l=10, r=40),
                legend=dict(orientation="v", y=1, x=0.6, xanchor="left", font=dict(color=text_color)
                ),
                paper_bgcolor=bg_color,
                plot_bgcolor=bg_color
                
            )

            for ann in fig.layout.annotations:
                ann.font.color = text_color

            fig.update_layout(annotations=[
                dict(text=f"<span style='font-size:23px;'><b>VALID</b></span><br><br><span style='font-size:40px;'>{values[0]}</span>",
                     x=0.25, y=1.22, xref="paper", yref="paper",
                     showarrow=False, align="center", font=dict(size=16, color=text_color)),
                dict(text=f"<span style='font-size:23px;'><b>INVALID</b></span><br><br><span style='font-size:40px;'>{values[1]}</span>",
                     x=0.50, y=1.22, xref="paper", yref="paper",
                     showarrow=False, align="center", font=dict(size=16, color=text_color)),
                dict(text=f"<span style='font-size:23px;'><b>NOT IN NMS</b></span><br><br><span style='font-size:40px;'>{values[2]}</span>",
                     x=0.75, y=1.22, xref="paper", yref="paper",
                     showarrow=False, align="center", font=dict(size=16, color=text_color)),
            ])

            st.plotly_chart(fig, use_container_width=True)

        with tab_tabular:
            tabular = get_unique(df)

            col1, col2, col3, col4 = st.columns([1.5, 1.5, 3, 1])
 
            with col1:
                sort_column = st.selectbox(
                "Sort by column:",
                options=tabular.columns, 
                index=0, 
                label_visibility="visible"
            )

            with col2:
                sort_order = st.radio(
                "Order:",
                options=["Ascending", "Descending"],
                horizontal=True
            )
            
            with col3:
                search_query = st.text_input(
                "Search by Site code or Alarm Count:",
                value="",
                label_visibility="visible"
            )
                
            with col4:
                with st.container():
                    st.markdown("<div style='padding-top: 1.8rem;'></div>", unsafe_allow_html=True)
                    clear_search = st.button("Clear Search")

            if clear_search:
                search_query = ""

            if search_query:
                search_query = search_query.strip().lower()
                tabular = tabular[
                    tabular["SITE CODE"].astype(str).str.lower().str.contains(search_query)
                    | tabular["Alarm Count"].astype(str).str.contains(search_query)
            ]

            ascending = sort_order == "Ascending"
            sorted_tabular = tabular.sort_values(by=sort_column, ascending=ascending)

            st.dataframe(sorted_tabular, use_container_width=True)

    st.markdown("---")
    st.subheader("🧠 Explore Your Data (Interactive)")
    pyg_app = StreamlitRenderer(st.session_state.df_pandas)
    pyg_app.explorer()


if __name__ == '__main__':
    main()
