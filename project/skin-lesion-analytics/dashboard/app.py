"""Streamlit dashboard for Skin Lesion Analytics."""

import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "warehouse" / "skin_lesions.duckdb"


@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


def query(sql: str) -> pd.DataFrame:
    return get_connection().execute(sql).fetchdf()


def main():
    st.set_page_config(page_title="Skin Lesion Analytics", layout="wide")
    st.title("Skin Lesion Analytics Dashboard")
    st.markdown(
        "Analysis of the **HAM10000** dermatoscopy dataset — "
        "10,015 skin lesion images across 7 diagnostic categories."
    )

    # ── Key Metrics ──────────────────────────────────────────────
    stats = query("""
        SELECT
            COUNT(*)                    AS total_images,
            COUNT(DISTINCT lesion_id)   AS unique_lesions,
            COUNT(DISTINCT diagnosis_code) AS diagnosis_types
        FROM stg_lesions
    """)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Images", f"{stats['total_images'].iloc[0]:,}")
    c2.metric("Unique Lesions", f"{stats['unique_lesions'].iloc[0]:,}")
    c3.metric("Diagnosis Types", stats['diagnosis_types'].iloc[0])

    # ── Tile 1: Diagnosis Distribution (categorical) ─────────────
    st.header("Diagnosis Distribution")

    diag = query("""
        SELECT diagnosis_name, total_cases, pct_of_total
        FROM diagnosis_summary
        ORDER BY total_cases DESC
    """)

    fig1 = px.bar(
        diag,
        x="diagnosis_name",
        y="total_cases",
        color="diagnosis_name",
        text="pct_of_total",
        labels={"diagnosis_name": "Diagnosis", "total_cases": "Cases"},
        title="Distribution of Skin Lesion Diagnoses",
    )
    fig1.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig1.update_layout(showlegend=False, xaxis_tickangle=-30)
    st.plotly_chart(fig1, width="stretch")

    # ── Tile 2: Age Group × Diagnosis (temporal / numerical) ─────
    st.header("Cases by Age Group and Diagnosis")

    demo = query("""
        SELECT age_group, diagnosis_name, SUM(case_count) AS cases
        FROM demographics_analysis
        GROUP BY age_group, diagnosis_name
        ORDER BY age_group
    """)

    fig2 = px.line(
        demo,
        x="age_group",
        y="cases",
        color="diagnosis_name",
        markers=True,
        labels={
            "age_group": "Age Group",
            "cases": "Number of Cases",
            "diagnosis_name": "Diagnosis",
        },
        title="Skin Lesion Cases Across Age Groups",
    )
    st.plotly_chart(fig2, width="stretch")

    # ── Tile 3: Body Location Breakdown ──────────────────────────
    st.header("Lesions by Body Location")

    loc = query("""
        SELECT body_location, SUM(case_count) AS total_cases
        FROM body_location_analysis
        GROUP BY body_location
        ORDER BY total_cases DESC
        LIMIT 15
    """)

    fig3 = px.pie(
        loc,
        values="total_cases",
        names="body_location",
        title="Distribution of Lesions by Body Location",
    )
    st.plotly_chart(fig3, width="stretch")

    # ── Tile 4: Gender split per diagnosis ───────────────────────
    st.header("Diagnosis by Patient Sex")

    sex = query("""
        SELECT diagnosis_name, patient_sex, SUM(case_count) AS cases
        FROM demographics_analysis
        WHERE patient_sex != 'unknown'
        GROUP BY diagnosis_name, patient_sex
        ORDER BY diagnosis_name
    """)

    fig4 = px.bar(
        sex,
        x="diagnosis_name",
        y="cases",
        color="patient_sex",
        barmode="group",
        labels={
            "diagnosis_name": "Diagnosis",
            "cases": "Cases",
            "patient_sex": "Sex",
        },
        title="Diagnosis Counts by Patient Sex",
    )
    fig4.update_layout(xaxis_tickangle=-30)
    st.plotly_chart(fig4, width="stretch")


if __name__ == "__main__":
    main()
