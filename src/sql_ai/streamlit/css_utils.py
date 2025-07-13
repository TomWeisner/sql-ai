import streamlit as st


def set_sidebar_width_and_center_content(
    sidebar_width: int = 400,
    max_content_width: int = 1100,
    padding_rem: float = 0.25,
):
    """
    Set the sidebar width and center the main content.

    Args:
        sidebar_width (int, optional): The width of the sidebar in pixels.
        Defaults to 400.
        max_content_width (int, optional): The maximum width of the main
        content in pixels. Defaults to 1100.

    Notes:
        This function is intended to be used within a Streamlit app. It sets
        the sidebar width to the specified value and centers the main content
        within the available space. The main content is also given a small
        amount of padding on either side.
    """
    st.markdown(
        f"""
        <style>
            /* Fix sidebar width */
            [data-testid="stSidebar"] {{
                min-width: {sidebar_width}px;
                max-width: {sidebar_width}px;
                width: {sidebar_width}px;
                padding-top: 0rem;
            }}

            /* Center the main content */
            .block-container {{
                max-width: {max_content_width}px;
                margin-left: auto;
                margin-right: auto;
                padding-left: {padding_rem}rem;
                padding-right: {padding_rem}rem;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def set_title_top_padding(rem: float = 1.0):
    """Adjusts the vertical spacing above st.title."""
    st.markdown(
        f"""
        <style>
            .block-container {{
                padding-top: {rem}rem !important;
            }}
            h1 {{
                margin-top: 0rem;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )
