import streamlit as st
import snowflake.snowpark as snowpark
import pandas as pd
import altair as alt

def main():
    st.title("Snowflake FinOps Usage Dashboard")

    # Sidebar filters
    st.sidebar.header("Filters")

    # Month filter (free text)
    selected_month = st.sidebar.text_input("Filter by month (YYYYMM)", value="")

    # Get Snowpark session
    session = snowpark.Session.builder.getOrCreate()

    # SQL query
    query = "SELECT * FROM dev_front.finops_bronze.snowflake_finops"

    try:
        # Run query
        snowpark_df = session.sql(query)

        # Collect results into list of Rows
        results = snowpark_df.collect()

        # Convert to pandas DataFrame
        df = pd.DataFrame([row.as_dict() for row in results])

        if "MAAND" not in df.columns:
            st.error("The dataset does not contain a 'MAAND' column.")
            return

        # Convert MAAND (YYYYMM) to datetime
        try:
            df["maand"] = pd.to_datetime(df["MAAND"], format="%Y%m")
        except Exception:
            df["maand"] = pd.to_datetime(df["MAAND"])

        # Sort by maand
        df = df.sort_values("maand")

        # Apply month filter if input is valid
        filtered_month = None
        if selected_month:
            try:
                selected_date = pd.to_datetime(selected_month, format="%Y%m")
                filtered_month = selected_date
                df = df[df["maand"] == selected_date]
                if df.empty:
                    st.warning(f"No data found for {selected_month}.")
                    return
            except Exception:
                st.warning("Invalid month format. Please use YYYYMM (e.g., 202501 for January 2025).")
                return

        # Detect numeric columns
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        if not numeric_cols:
            st.error("No numeric measures found in the dataset.")
            return

        # Sidebar checkboxes for measures
        st.sidebar.write("Select measures to display:")
        selected_measures = [
            col for col in numeric_cols
            if st.sidebar.checkbox(col, value=True)
        ]

        if not selected_measures:
            st.warning("Please select at least one measure to display.")
            return

        # Filter dataframe
        df_filtered = df[["maand"] + selected_measures]

        # Collapsible dataframe
        with st.expander("📊 Show/Hide FinOps DataFrame", expanded=False):
            st.dataframe(df_filtered)

        # Line chart
        st.subheader("Monthly Credits Usage Trend")

        df_melted = df_filtered.melt(
            id_vars=["maand"],
            value_vars=selected_measures,
            var_name="metric",
            value_name="value"
        )

        chart = (
            alt.Chart(df_melted)
            .mark_line(point=True)
            .encode(
                x=alt.X("maand:T", title="Month", axis=alt.Axis(format="%b %Y")),
                y=alt.Y("value:Q", title="Credits", scale=alt.Scale(zero=False, nice=True)),
                color=alt.Color(
                    "metric:N",
                    legend=alt.Legend(
                        orient="bottom",
                        direction="horizontal",
                        columns=1,
                        title="Metric",
                        labelFontSize=12,
                        labelLimit=1000
                    )
                )
            )
            .properties(width='container', height=750)
        )

        st.altair_chart(chart, use_container_width=True)

        # Static legend
        st.subheader("Legend for Metric Abbreviations")
        legend_list = [
            "wmh - warehouse metering history",
            "mdh - metering daily history"
        ]
        for item in legend_list:
            st.markdown(f"- {item}")

        # If one month is selected → show a bar chart
        if filtered_month is not None:
            st.subheader(f"Bar Chart for {filtered_month.strftime('%B %Y')}")

            row = df_filtered.iloc[0]  # Only one row for this month
            bar_data = pd.DataFrame({
                "metric": selected_measures,
                "value": [row[m] for m in selected_measures]
            })

            bar_chart = (
                alt.Chart(bar_data)
                .mark_bar()
                .encode(
                    x=alt.X("metric:N", title="Metric", sort="-y"),
                    y=alt.Y(
                        "value:Q",
                        title="Value",
                        scale=alt.Scale(zero=False, nice=True)  # ← dynamic scaling
                    ),
                    color=alt.Color(
                        "metric:N",
                        legend=alt.Legend(
                            orient="bottom",
                            direction="horizontal",
                            columns=1,
                            title="Metric",
                            labelLimit=1000
                        )
                    ),
                    tooltip=["metric", "value"]
                )
                .properties(width='container', height=600)  # ← increased height
            )

            st.altair_chart(bar_chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error executing query: {e}")

if __name__ == "__main__":
    main()
