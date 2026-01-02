from __future__ import annotations

import html


def user_message_html(question: str) -> str:
    question_text = html.escape(question)
    return f"""
    <div class="user-row-wrap">
        <div class="user-row">
            <span class="user-text">{question_text}</span>
        </div>
    </div>
    """


def message_time_html(timestamp: str, side: str) -> str:
    return f"<div class='message-time {side}'>{timestamp}</div>"


def table_meta_html(label: str, value: str) -> str:
    return (
        "<div class='table-meta'>"
        f"<span class='meta-label'>{html.escape(label)}</span>={html.escape(value)}"
        "</div>"
    )


def table_desc_html(desc_html: str) -> str:
    return f"<div class='table-desc'>{desc_html}</div>"


def button_class_script_html() -> str:
    return """
    <script>
    const root = window.parent.document;
    const buttons = root.querySelectorAll('button');
    buttons.forEach((btn) => {
      const text = (btn.innerText || '').trim().toLowerCase();
      if (text === '🗑️') {
        btn.classList.add('btn-clear');
      }
      if (text === '🔁') {
        btn.classList.add('btn-retry');
      }
      if (text === '🗂️' || text === '🔍' || text === '🙈') {
        btn.classList.add('btn-tabs');
      }
    });
    </script>
    """


def toggle_class_script_html() -> str:
    return """
    <script>
    const root = window.parent.document;
    const labels = root.querySelectorAll('[data-testid="stCheckbox"] label');
    labels.forEach((label) => {
      const text = (label.innerText || '').trim().toLowerCase();
      if (text.startsWith('keep memory')) {
        label.classList.add('toggle-keep');
      }
      if (text.startsWith('show tabs')) {
        label.classList.add('toggle-show-tabs');
      }
      if (text.startsWith('dry run')) {
        label.classList.add('toggle-dry-run');
      }
      if (text.startsWith('supplied sql')) {
        label.classList.add('toggle-supplied');
      }
    });
    </script>
    """
