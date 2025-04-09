import streamlit as st
import pandas as pd
import plotly.express as px
import os
import time

# File path
DATA_FILE = "consumer_output.json"

# Streamlit Page Config
st.set_page_config(page_title="🐾 Animal Classification Dashboard", layout="wide")

# Load Data Function
@st.cache_data(ttl=5)
def load_data():
    try:
        df = pd.read_json(DATA_FILE)
        if df.empty:
            return pd.DataFrame(columns=["file_name", "species", "accuracy", "file_size_kb", "timestamp"])
        df["accuracy"] = df["accuracy"].round(2)
        df["file_size_kb"] = df.get("file_size_kb", 0).round(2)
        df = df.tail(10)  # Last 10 records
        return df
    except (ValueError, FileNotFoundError):
        return pd.DataFrame(columns=["file_name", "species", "accuracy", "file_size_kb", "timestamp"])

# Load Full Data for Totals
def load_full_data():
    try:
        df_all = pd.read_json(DATA_FILE)
        df_all["accuracy"] = df_all["accuracy"].round(2)
        df_all["file_size_kb"] = df_all.get("file_size_kb", 0).round(2)
        return df_all
    except (ValueError, FileNotFoundError):
        return pd.DataFrame(columns=["file_name", "species", "accuracy", "file_size_kb", "timestamp"])

# Export CSV
def convert_df(df):
    return df.to_csv(index=False).encode("utf-8")

# Highlight Accuracy
def highlight_accuracy(val):
    return "background-color: red; color: white;" if val < 50 else ""

# 🔍 Recursive search for image file
def find_image_path(filename, root_dir="archive/animals/animals"):
    for dirpath, _, filenames in os.walk(root_dir):
        filenames_lower = [f.lower() for f in filenames]
        if filename.lower() in filenames_lower:
            index = filenames_lower.index(filename.lower())
            found_path = os.path.join(dirpath, filenames[index])
            return found_path
    return None

# Main App
def main():
    st.title("📊 Animal Classification Dashboard")

    while True:
        st.toast("🔄 Auto-refreshing every 10 seconds...")

        # Load data
        data = load_data()
        full_data = load_full_data()

        # Search
        search_term = st.text_input("🔍 Search species", "").lower()
        filtered_data = data[data["species"].str.lower().str.contains(search_term, na=False)]

        # CSV Export
        csv = convert_df(data)
        st.download_button("📥 Export CSV", csv, "detections.csv", "text/csv")

        # Total Count (All Time)
        st.subheader("📌 Total Detections (All Time)")
        st.write(f"Total classified images: **{len(full_data)}**")

        # Species Count (Top 10)
        st.subheader("📊 Species Count (Top 10)")
        species_count = full_data["species"].value_counts().head(10)
        species_count_df = pd.DataFrame({
            "Species": species_count.index,
            "Count": species_count.values
        })
        st.table(species_count_df)

        # Latest Detections Table
        st.subheader("🧾 Latest Detections (Last 10)")
        if "file_size_kb" not in filtered_data.columns:
            filtered_data["file_size_kb"] = "N/A"
        styled_data = filtered_data.style.applymap(highlight_accuracy, subset=["accuracy"])
        st.dataframe(styled_data)

        # 🖼️ Updated Image Previews (horizontal layout)
        st.subheader("🖼️ Image Previews")
        cols = st.columns(5)  # 5 columns per row
        col_index = 0

        for _, row in filtered_data.iterrows():
            image_path = find_image_path(row["file_name"])
            if image_path:
                with cols[col_index]:
                    st.image(image_path, caption=f"{row['species']} ({row['accuracy']}%)", use_container_width=True)
                col_index = (col_index + 1) % 5
                if col_index == 0:
                    cols = st.columns(5)  # Start a new row
            else:
                st.warning(f"⚠️ Image not found for: {row['file_name']}")

        # Charts
        if not data.empty:
            fig_pie = px.pie(
                names=species_count.index,
                values=species_count.values,
                title="🔢 Species Distribution"
            )
            fig_bar = px.bar(
                data,
                x="species",
                y="accuracy",
                title="🎯 Accuracy Distribution",
                color="species"
            )
            fig_line = px.line(
                data,
                x="timestamp",
                y="accuracy",
                title="📈 Accuracy Over Time"
            )
            fig_size = px.bar(
                data,
                x="file_name",
                y="file_size_kb",
                title="🧮 File Size of Images (KB)",
                color="species"
            )

            col1, col2 = st.columns(2)
            col1.plotly_chart(fig_pie, use_container_width=True)
            col2.plotly_chart(fig_bar, use_container_width=True)
            st.plotly_chart(fig_line, use_container_width=True)
            st.plotly_chart(fig_size, use_container_width=True)

        # Refresh
        time.sleep(10)
        st.rerun()

# Run app
if __name__ == "__main__":
    main()
