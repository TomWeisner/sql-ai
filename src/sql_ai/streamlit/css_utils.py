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


def inject_app_styles(chat_width: int = 640):
    """Inject app-specific CSS styles."""
    st.markdown(
        f"""
        <style>
        :root {{
            --chat-width: {chat_width}px;
        }}
        .stMarkdown,
        .stMarkdown > div {{
            width: 100%;
        }}
        .block-container {{
            max-width: var(--chat-width);
        }}
        .user-row {{
            display: inline-flex;
            justify-content: flex-end;
            align-items: center;
            gap: 10px;
            background: #f6f7f9;
            border-radius: 999px;
            padding: 14px 16px;
            margin: 6px 0 12px 0;
            max-width: 100%;
        }}
        .user-row-wrap {{
            display: flex;
            justify-content: flex-end;
            width: 100%;
        }}
        .user-row .user-text {{
            text-align: right;
            font-weight: 500;
        }}
        .chat-action-row {{
            display: inline-flex;
            gap: 6px;
            align-items: center;
            width: 100%;
        }}
        .message-time {{
            font-size: 0.75rem;
            color: #9ca3af;
            margin-top: -6px;
            line-height: 1;
        }}
        .message-time.right {{
            text-align: right;
            padding-right: 10px;
        }}
        .message-time.left {{
            text-align: left;
            margin-top: -10px;
        }}
        .stCaption,
        .stCaption p {{
            color: #111827 !important;
        }}
        div[data-testid="stHorizontalBlock"] [data-testid="stCheckbox"] {{
            display: flex;
            justify-content: center;
        }}
        div[data-testid="stHorizontalBlock"] [data-testid="stCheckbox"] > label {{
            justify-content: center;
            width: 100%;
            margin: 0 auto;
        }}
        div[data-testid="stHorizontalBlock"] [data-testid="stCheckbox"] > label > div {{
            justify-content: center;
        }}
        div[data-testid="stHorizontalBlock"] [data-testid="stCheckbox"] span {{
            text-align: center;
        }}
        div[data-testid="stCheckbox"] input[type="checkbox"] {{
            accent-color: #86efac;
        }}
        div[data-testid="stCheckbox"] div[data-baseweb="checkbox"] svg path {{
            fill: #86efac;
            stroke: #86efac;
        }}
        .table-meta {{
            font-size: 0.9rem;
        }}
        .table-meta .meta-label {{
            color: #2563eb;
            font-weight: 600;
        }}
        .table-desc {{
            color: #111827;
            font-size: 0.9rem;
            line-height: 1.4;
            border-top: 1px solid #ffffff;
            border-bottom: 1px solid #ffffff;
            padding: 8px 0;
            margin: 6px 0 10px 0;
        }}
        .table-desc code {{
            color: #16a34a;
            background: #f3f4f6;
            padding: 2px 6px;
            border-radius: 6px;
            font-size: 0.85rem;
        }}
        section[data-testid="stSidebar"] code {{
            color: #16a34a;
            background: #f7f8fa;
            padding: 2px 6px;
            border-radius: 6px;
            font-size: 0.85rem;
        }}
        section[data-testid="stSidebar"] div[data-testid="stExpander"] > details {{
            background: #ffffff;
        }}
        [data-testid="stTextInput"] input {{
            border-radius: 999px;
            padding: 0.65rem 0.95rem;
            border: 1px solid #e5e7eb;
            background: #f3f4f6;
            width: 100%;
        }}
        div[data-testid="stChatInput"] {{
            max-width: var(--chat-width);
            margin-left: auto;
            margin-right: auto;
        }}
        div[data-testid="stChatInput"] > div {{
            max-width: var(--chat-width);
            margin-left: auto;
            margin-right: auto;
        }}
        [data-testid="stTextInput"] div[data-baseweb="base-input"] {{
            background: transparent;
            border: none;
        }}
        [data-testid="stTextInput"] div[data-baseweb="base-input"] > div {{
            background: transparent;
        }}
        [data-testid="stForm"] button {{
            border-radius: 999px;
            padding: 0.55rem 0.8rem;
            border: 1px solid #e5e7eb;
            background: #f3f4f6;
        }}
        [data-testid="stButton"] button {{
            white-space: nowrap;
            width: 100%;
        }}
        button.btn-clear,
        button.btn-retry,
        button.btn-tabs {{
            border: none;
            background: transparent;
            box-shadow: none;
            padding: 6px 8px;
            border-radius: 10px;
            font-size: 1rem;
            font-weight: 500;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }}
        button.btn-clear:hover {{
            background: #fee2e2;
            color: #991b1b;
        }}
        button.btn-retry:hover {{
            background: #dcfce7;
            color: #166534;
        }}
        button.btn-tabs:hover {{
            background: #e0f2fe;
            color: #0c4a6e;
        }}
        div[data-testid="stExpander"] {{
            border: 1px solid #e5e7eb;
            box-shadow: none;
            max-width: var(--chat-width);
            margin-left: 0;
        }}
        div[data-testid="stExpander"] > details {{
            border: none;
            box-shadow: none;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )
