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


def copy_button_script_html() -> str:
    return """
    <script>
    (function() {
      const root = window.parent.document;
      const markCopied = (btn) => {
        btn.classList.add('copied');
        const prevTitle = btn.getAttribute('title') || '';
        btn.setAttribute('data-prev-title', prevTitle);
        btn.setAttribute('title', 'Copied!');
        window.clearTimeout(btn._copiedTimeout);
        btn._copiedTimeout = window.setTimeout(() => {
          btn.classList.remove('copied');
          const old = btn.getAttribute('data-prev-title');
          if (old !== null) {
            btn.setAttribute('title', old);
            btn.removeAttribute('data-prev-title');
          }
        }, 1200);
      };

      const shouldAttach = (btn) => {
        const aria = (btn.getAttribute('aria-label') || '').toLowerCase();
        const title = (btn.getAttribute('title') || '').toLowerCase();
        const testid = (btn.getAttribute('data-testid') || '').toLowerCase();
        return (
          testid.includes('copy') ||
          aria.includes('copy') ||
          title.includes('copy')
        );
      };

      const wire = () => {
        const buttons = root.querySelectorAll('button');
        buttons.forEach((btn) => {
          if (!shouldAttach(btn)) return;
          if (btn.dataset.copyListenerAttached === 'true') return;
          btn.dataset.copyListenerAttached = 'true';
          btn.addEventListener('click', () => markCopied(btn));
        });
      };

      wire();
      const observer = new MutationObserver(() => wire());
      observer.observe(root.body, { childList: true, subtree: true });
    })();
    </script>
    """


def chat_input_reset_script_html() -> str:
    return """
    <script>
    (function() {
      const root = window.parent.document;
      const MIN_HEIGHT = 48;

      const sizeTextarea = (textarea, forceMin=false) => {
        if (!textarea) return;
        const isEmpty = !textarea.value || textarea.value.trim() === '';
        if (forceMin || isEmpty) {
          textarea.style.height = MIN_HEIGHT + 'px';
          textarea.style.overflowY = 'hidden';
          textarea.rows = 1;
          return;
        }
        textarea.style.height = 'auto';
        textarea.style.height = textarea.scrollHeight + 'px';
        textarea.style.overflowY = 'hidden';
      };

      const wireTextarea = () => {
        const textarea = root.querySelector('div[data-testid="stChatInput"] textarea');
        if (!textarea || textarea.dataset.resetListenerAttached === 'true') return;
        textarea.dataset.resetListenerAttached = 'true';
        textarea.addEventListener('input', () => sizeTextarea(textarea));
        textarea.addEventListener('paste', () => {
          window.setTimeout(() => sizeTextarea(textarea), 0);
        });
        textarea.addEventListener('keydown', (e) => {
          if (e.key === 'Enter' && !e.shiftKey) {
            window.setTimeout(() => sizeTextarea(textarea, true), 80);
          }
        });
        sizeTextarea(textarea, true);
      };

      const wireSendButton = () => {
        const sendBtn = root.querySelector('div[data-testid="stChatInput"] button');
        const textarea = root.querySelector('div[data-testid="stChatInput"] textarea');
        if (sendBtn && !sendBtn.dataset.resetListenerAttached) {
          sendBtn.dataset.resetListenerAttached = 'true';
          sendBtn.addEventListener('click', () => {
            window.setTimeout(() => sizeTextarea(textarea, true), 80);
          });
        }
      };

      const wire = () => {
        wireTextarea();
        wireSendButton();
      };

      wire();
      const observer = new MutationObserver(() => wire());
      observer.observe(root.body, { childList: true, subtree: true });
      window.setInterval(() => {
        const textarea = root.querySelector('div[data-testid="stChatInput"] textarea');
        if (textarea && (!textarea.value || textarea.value.trim() === '')) {
          sizeTextarea(textarea, true);
        }
      }, 300);
    })();
    </script>
    """
