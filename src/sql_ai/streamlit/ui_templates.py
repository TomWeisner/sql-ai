"""HTML/CSS/JS template snippets for the Streamlit UI."""

from __future__ import annotations

import html
import json
import re


def user_message_html(question: str, attrs: str = "") -> str:
    question_text = html.escape(question)
    return f"""
    <div class="user-row-wrap" {attrs}>
        <div class="user-row">
            <span class="user-text">{question_text}</span>
        </div>
    </div>
    """


def message_time_html(timestamp: str, side: str) -> str:
    return f"<div class='message-time {side}'>{timestamp}</div>"


def assistant_answer_html(message: str, pulse: bool = False) -> str:
    escaped = html.escape(message)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    escaped = escaped.replace("\n", "<br>")
    pulse_class = " assistant-answer--pulse" if pulse else ""
    return f"<div class='assistant-answer{pulse_class}'>{escaped}</div>"


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
    (function() {
    const root = window.parent.document;
    const parentWindow = window.parent;

    const scrollToElement = (target) => {
      if (!target) return;
      const rect = target.getBoundingClientRect();
      const absoluteTop = rect.top + parentWindow.pageYOffset;
      parentWindow.scrollTo({ top: absoluteTop, behavior: 'smooth' });
    };

    const scrollToTop = () => {
      parentWindow.scrollTo({ top: 0, behavior: 'smooth' });
    };

    const wire = () => {
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
          if (text.startsWith('⬆️')) {
            btn.classList.add('btn-scroll');
          }
          if (text.startsWith('▶️') || text.startsWith('🧠')) {
            btn.classList.add('btn-action');
          }
          if (text.includes('run sql') || text.includes('interpret')) {
            btn.classList.add('btn-action');
          }
          if (text.includes('run sql')) {
            btn.classList.add('btn-action-run');
          }
          if (text.includes('interpret')) {
            btn.classList.add('btn-action-interpret');
          }
          if (btn.classList.contains('footer-scroll-btn')) {
            btn.classList.add('btn-scroll');
            if (btn.dataset.scrollListenerAttached === 'true') return;
            btn.dataset.scrollListenerAttached = 'true';
            btn.addEventListener('click', () => {
              const targetKind = btn.getAttribute('data-scroll-target') || '';
              let target = null;
              if (targetKind === 'page-top') {
                target = root.getElementById('page-top');
                if (target) {
                  scrollToElement(target);
                } else {
                  scrollToTop();
                }
                return;
              } else {
                target = root.querySelector('[data-current-question="true"]')
                  || root.querySelector('[data-question-anchor="true"]');
              }
              if (target) {
                scrollToElement(target);
              }
            });
          }
          if (text.startsWith('▶️') || text.startsWith('🧠')) {
            if (btn.dataset.actionListenerAttached === 'true') return;
            btn.dataset.actionListenerAttached = 'true';
            btn.addEventListener('click', () => {
              const label = (btn.innerText || '').trim();
              if (label.toLowerCase().startsWith('▶️')) {
                btn.innerText = 'Running SQL...';
              } else if (label.toLowerCase().startsWith('🧠')) {
                btn.innerText = 'Interpreting...';
              }
              btn.classList.add('btn-busy');
              btn.setAttribute('disabled', 'true');
            });
          }
        });
      };
      wire();
      const observer = new MutationObserver(() => wire());
      observer.observe(root.body, { childList: true, subtree: true });
    })();
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


def tab_scroll_lock_script_html() -> str:
    return """
    <script>
    (function() {
      const root = window.parent.document;
      let lastScroll = 0;

      const getScrollTop = () =>
        root.documentElement.scrollTop || root.body.scrollTop || 0;

      const setScrollTop = (value) => {
        root.documentElement.scrollTo({ top: value, behavior: 'auto' });
        root.body.scrollTop = value;
      };

      const wire = () => {
        const tabs = root.querySelectorAll('[data-testid="stTabs"] [role="tab"]');
        tabs.forEach((tab) => {
          if (tab.dataset.scrollLockAttached === 'true') return;
          tab.dataset.scrollLockAttached = 'true';
          tab.addEventListener('mousedown', () => {
            lastScroll = getScrollTop();
          }, { passive: true });
          tab.addEventListener('click', () => {
            const prev = lastScroll;
            window.setTimeout(() => setScrollTop(prev), 0);
            window.setTimeout(() => setScrollTop(prev), 60);
          });
        });
      };

      wire();
      const observer = new MutationObserver(() => wire());
      observer.observe(root.body, { childList: true, subtree: true });
    })();
    </script>
    """


def scroll_controls_script_html() -> str:
    return """
    <script>
    (function() {
      const root = window.parent.document;

      const getScrollContainers = () => {
        const containers = [];
        const appView = root.querySelector('div[data-testid="stAppViewContainer"]');
        const main = root.querySelector('section[data-testid="stMain"]');
        const scrolling = root.scrollingElement || root.documentElement;
        [appView, main, scrolling, root.body].forEach((el) => {
          if (!el) return;
          const style = root.defaultView.getComputedStyle(el);
          const canScroll = el.scrollHeight > el.clientHeight + 2;
          const overflowOk = style.overflowY !== 'hidden';
          if (canScroll && overflowOk && !containers.includes(el)) {
            containers.push(el);
          }
        });
        return containers;
      };

      const scrollToTarget = (target) => {
        if (!target) return;
        const containers = getScrollContainers();
        if (!containers.length) {
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
          return;
        }

        const start = performance.now();
        const tick = () => {
          containers.forEach((container) => {
            const containerRect = container.getBoundingClientRect();
            const targetRect = target.getBoundingClientRect();
            const offset = targetRect.top - containerRect.top + container.scrollTop;
            container.scrollTo({ top: offset, behavior: 'auto' });
          });
          if (performance.now() - start < 240) {
            requestAnimationFrame(tick);
          } else {
            const first = containers[0];
            const containerRect = first.getBoundingClientRect();
            const targetRect = target.getBoundingClientRect();
            const offset = targetRect.top - containerRect.top + first.scrollTop;
            first.scrollTo({ top: offset, behavior: 'smooth' });
          }
        };
        tick();
      };

      const ensure = () => {
        let container = root.getElementById('scroll-controls');
        if (!container) {
          container = root.createElement('div');
          container.id = 'scroll-controls';
          container.className = 'scroll-controls';

          const topBtn = root.createElement('button');
          topBtn.type = 'button';
          topBtn.className = 'scroll-btn';
          topBtn.textContent = '⬆️ Top';

          const currentBtn = root.createElement('button');
          currentBtn.type = 'button';
          currentBtn.className = 'scroll-btn';
          currentBtn.textContent = '↩️ Question';

          container.appendChild(topBtn);
          container.appendChild(currentBtn);
          root.body.appendChild(container);

          topBtn.addEventListener('click', () => {
            scrollToTarget(root.getElementById('conversation-top'));
          });

          currentBtn.addEventListener('click', () => {
            const target = root.querySelector('[data-current-question="true"]')
              || root.querySelector('[data-question-anchor="true"]');
            scrollToTarget(target);
          });
        }

        const hasConversation = !!root.getElementById('conversation-top');
        container.style.display = hasConversation ? 'flex' : 'none';
      };

      ensure();
      const observer = new MutationObserver(() => ensure());
      observer.observe(root.body, { childList: true, subtree: true });
    })();
    </script>
    """


def scroll_to_anchor_script_html(selector: str, token: int) -> str:
    return f"""
    <script>
    (function() {{
      const root = window.parent.document;
      const selector = {json.dumps(selector)};
      const token = {token};

      const getScrollContainers = () => {{
        const containers = [];
        const appView = root.querySelector('div[data-testid="stAppViewContainer"]');
        const main = root.querySelector('section[data-testid="stMain"]');
        const scrolling = root.scrollingElement || root.documentElement;
        [appView, main, scrolling, root.body].forEach((el) => {{
          if (!el) return;
          const style = root.defaultView.getComputedStyle(el);
          const canScroll = el.scrollHeight > el.clientHeight + 2;
          const overflowOk = style.overflowY !== 'hidden';
          if (canScroll && overflowOk && !containers.includes(el)) {{
            containers.push(el);
          }}
        }});
        return containers;
      }};

      const scrollToTarget = () => {{
        const containers = getScrollContainers();
        if (!containers.length) {{
          target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
          return;
        }}

        const start = performance.now();
        const tick = () => {{
          containers.forEach((container) => {{
            const containerRect = container.getBoundingClientRect();
            const targetRect = target.getBoundingClientRect();
            const offset = targetRect.top - containerRect.top + container.scrollTop;
            container.scrollTo({{ top: offset, behavior: 'auto' }});
          }});
          if (performance.now() - start < 240) {{
            requestAnimationFrame(tick);
          }} else {{
            const first = containers[0];
            const containerRect = first.getBoundingClientRect();
            const targetRect = target.getBoundingClientRect();
            const offset = targetRect.top - containerRect.top + first.scrollTop;
            first.scrollTo({{ top: offset, behavior: 'smooth' }});
          }}
        }};
        tick();
        }};

      const attempt = (tries=0) => {{
        const target = selector === 'PAGE_TOP' ? root.body : root.querySelector(selector);
        if (!target) {{
          if (tries < 20) {{
            window.setTimeout(() => attempt(tries + 1), 50);
          }}
          return;
        }}
        if (selector === 'PAGE_TOP') {{
          const containers = getScrollContainers();
          if (!containers.length) {{
            root.defaultView.scrollTo({{ top: 0, behavior: 'smooth' }});
          }} else {{
            containers.forEach((container) => {{
              container.scrollTo({{ top: 0, behavior: 'smooth' }});
            }});
          }}
          root.documentElement.scrollTo({{ top: 0, behavior: 'smooth' }});
          root.body.scrollTo({{ top: 0, behavior: 'smooth' }});
          return;
        }}
        scrollToTarget(target);
      }};

      window.setTimeout(() => attempt(), 0);
    }})();
    </script>
    """
