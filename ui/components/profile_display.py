"""Profile display component."""
import streamlit as st
import pandas as pd

from profiler.table_profiler import TableProfile


def render_profile_display(profile: TableProfile) -> None:
    """
    Render the profile results display.

    Args:
        profile: The TableProfile to display
    """
    # Table overview
    st.subheader("Table Overview")
    _render_overview(profile)

    # Column statistics
    st.subheader("Column Statistics")
    _render_column_stats(profile)

    # Sample data
    if profile.sample_data is not None and not profile.sample_data.empty:
        st.subheader("Sample Data")
        st.dataframe(profile.sample_data, use_container_width=True)


def _render_overview(profile: TableProfile) -> None:
    """Render table overview metrics."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Rows", f"{profile.row_count:,}")

    with col2:
        st.metric("Columns", len(profile.columns))

    with col3:
        if profile.size_bytes:
            size_mb = profile.size_bytes / (1024 * 1024)
            if size_mb >= 1024:
                st.metric("Size", f"{size_mb / 1024:.2f} GB")
            else:
                st.metric("Size", f"{size_mb:.2f} MB")
        else:
            st.metric("Size", "N/A")

    with col4:
        if profile.table_info.modified:
            st.metric("Last Modified", profile.table_info.modified[:10])
        else:
            st.metric("Last Modified", "N/A")

    if profile.table_info.description:
        st.info(f"**Description:** {profile.table_info.description}")


def _render_column_stats(profile: TableProfile) -> None:
    """Render column statistics table."""
    # Build summary dataframe
    data = []
    for stat in profile.column_stats:
        row = {
            "Column": stat.column_name,
            "Type": stat.data_type,
            "Null %": f"{stat.null_percentage:.1f}%",
            "Distinct": f"{stat.distinct_count:,}",
            "Distinct %": f"{stat.distinct_percentage:.1f}%",
        }

        # Add min/max based on type
        if stat.min_value is not None:
            row["Min"] = _format_value(stat.min_value)
        else:
            row["Min"] = "-"

        if stat.max_value is not None:
            row["Max"] = _format_value(stat.max_value)
        else:
            row["Max"] = "-"

        data.append(row)

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Detailed view per column
    with st.expander("Detailed Column Statistics"):
        for stat in profile.column_stats:
            _render_column_detail(stat)
            st.divider()


def _render_column_detail(stat) -> None:
    """Render detailed statistics for a single column."""
    st.markdown(f"### `{stat.column_name}` ({stat.data_type})")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Completeness**")
        st.write(f"- Total: {stat.total_count:,}")
        st.write(f"- Nulls: {stat.null_count:,} ({stat.null_percentage:.1f}%)")

    with col2:
        st.markdown("**Uniqueness**")
        st.write(f"- Distinct: {stat.distinct_count:,}")
        st.write(f"- Distinct %: {stat.distinct_percentage:.1f}%")

    with col3:
        st.markdown("**Values**")
        if stat.min_value is not None:
            st.write(f"- Min: {_format_value(stat.min_value)}")
        if stat.max_value is not None:
            st.write(f"- Max: {_format_value(stat.max_value)}")
        if stat.avg_value is not None:
            st.write(f"- Avg: {stat.avg_value:.2f}")

    # String length stats
    if stat.min_length is not None:
        st.markdown("**String Length**")
        st.write(f"- Min: {stat.min_length}, Max: {stat.max_length}, Avg: {stat.avg_length:.1f}")

    # Top values
    if stat.top_values:
        st.markdown("**Top Values**")
        top_df = pd.DataFrame(stat.top_values, columns=["Value", "Count"])
        st.dataframe(top_df, use_container_width=True, hide_index=True)


def _format_value(value) -> str:
    """Format a value for display."""
    if value is None:
        return "-"
    if isinstance(value, float):
        if abs(value) >= 1e6:
            return f"{value:.2e}"
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    # Truncate long strings
    str_val = str(value)
    if len(str_val) > 50:
        return str_val[:47] + "..."
    return str_val
