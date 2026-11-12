(function () {
  const API_INFO_URL = '/api/info';
  const WIDGET_ID = 'appVersionWidget';
  const MODAL_ID = 'appVersionModal';
  const MAX_CHANGE_ITEMS = 5;

  const state = {
    info: null,
    loadStarted: false,
    loadError: null,
  };

  function escapeText(value) {
    return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function pickVersionInfo(payload) {
    const version = payload?.version || {};
    const latestChangelog = version.latest_changelog || {};
    const build = payload?.build || {};
    return {
      codeVersion: String(version.code_version || latestChangelog.version || build.code_version || '').trim(),
      title: String(version.title || latestChangelog.title || build.title || '').trim(),
      releasedAt: String(version.released_at || latestChangelog.released_at || '').trim(),
      changelogFile: String(version.changelog_file || '').trim(),
      buildTime: String(build.build_time || '').trim(),
      imageTag: String(build.image_tag || '').trim(),
      changes: Array.isArray(latestChangelog.changes) ? latestChangelog.changes.slice(0, MAX_CHANGE_ITEMS) : [],
      changeCount: Number(latestChangelog.change_count || 0),
    };
  }

  function formatVersionLabel(info) {
    return info?.codeVersion ? `v${info.codeVersion}` : '版本';
  }

  function updateWidget(widget) {
    if (!widget) return;
    const button = widget.querySelector('.app-version-trigger');
    const label = widget.querySelector('.app-version-code');
    const title = widget.querySelector('.app-version-title');
    const info = state.info ? pickVersionInfo(state.info) : null;
    if (label) label.textContent = formatVersionLabel(info);
    if (title) {
      title.textContent = info?.title || (state.loadError ? '更新日志不可用' : '正在读取更新日志');
    }
    if (button) {
      button.disabled = false;
      button.setAttribute('aria-label', info?.title ? `${formatVersionLabel(info)} ${info.title}` : '查看版本更新日志');
    }
    widget.dataset.state = state.loadError ? 'error' : (state.info ? 'ready' : 'loading');
  }

  async function loadVersionInfo(widget) {
    if (state.loadStarted) return;
    state.loadStarted = true;
    try {
      if (typeof loadApiInfo === 'function') {
        state.info = await loadApiInfo();
      } else {
        const response = await fetch(API_INFO_URL, {
          method: 'GET',
          cache: 'no-store',
          headers: { Accept: 'application/json' },
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        state.info = await response.json();
      }
      state.loadError = null;
    } catch (error) {
      state.loadError = error;
    } finally {
      updateWidget(widget);
    }
  }

  function renderMetaRow(label, value) {
    if (!value) return '';
    return `
      <div class="app-version-meta-row">
        <span>${escapeText(label)}</span>
        <strong>${escapeText(value)}</strong>
      </div>
    `;
  }

  function renderChangeList(info) {
    const changes = Array.isArray(info?.changes) ? info.changes.filter((item) => String(item || '').trim()) : [];
    if (!changes.length) {
      return '<div class="app-version-empty">本版本暂无可展示的更新摘要。</div>';
    }
    return `
      <ul class="app-version-change-list">
        ${changes.map((item) => `<li>${escapeText(item)}</li>`).join('')}
      </ul>
    `;
  }

  function buildModalContent() {
    const info = state.info ? pickVersionInfo(state.info) : null;
    const title = info?.title || (state.loadError ? '更新日志不可用' : '正在读取更新日志');
    const metaHtml = info ? [
      renderMetaRow('发布时间', info.releasedAt),
      renderMetaRow('构建时间', info.buildTime),
      renderMetaRow('镜像', info.imageTag),
      renderMetaRow('日志文件', info.changelogFile),
    ].join('') : '';
    const changeCountText = info?.changeCount ? `共 ${info.changeCount} 项，展示最近 ${Math.min(info.changeCount, MAX_CHANGE_ITEMS)} 项` : '简要更新';

    return `
      <div class="app-version-modal-card" role="dialog" aria-modal="true" aria-labelledby="appVersionModalTitle">
        <div class="app-version-modal-head">
          <div>
            <div class="app-version-modal-kicker">Fund-ANA</div>
            <h2 id="appVersionModalTitle">${escapeText(formatVersionLabel(info))}</h2>
          </div>
          <button class="app-version-close" type="button" aria-label="关闭更新日志">&times;</button>
        </div>
        <div class="app-version-release-title">${escapeText(title)}</div>
        ${metaHtml ? `<div class="app-version-meta">${metaHtml}</div>` : ''}
        <div class="app-version-section-title">${escapeText(changeCountText)}</div>
        ${renderChangeList(info)}
      </div>
    `;
  }

  function closeModal() {
    const modal = document.getElementById(MODAL_ID);
    if (!modal) return;
    modal.remove();
    document.removeEventListener('keydown', handleModalKeydown);
  }

  function handleModalKeydown(event) {
    if (event.key === 'Escape') {
      closeModal();
    }
  }

  function openModal() {
    closeModal();
    const modal = document.createElement('div');
    modal.id = MODAL_ID;
    modal.className = 'app-version-modal';
    modal.innerHTML = buildModalContent();
    modal.addEventListener('click', (event) => {
      if (event.target === modal || event.target?.classList?.contains('app-version-close')) {
        closeModal();
      }
    });
    document.body.appendChild(modal);
    document.addEventListener('keydown', handleModalKeydown);
    modal.querySelector('.app-version-close')?.focus();
  }

  function mountWidget() {
    if (document.getElementById(WIDGET_ID)) return;
    const shell = document.querySelector('.site-header .site-shell');
    if (!shell) return;
    const widget = document.createElement('div');
    widget.id = WIDGET_ID;
    widget.className = 'app-version-widget';
    widget.dataset.state = 'loading';
    widget.innerHTML = `
      <button class="app-version-trigger" type="button" aria-haspopup="dialog" aria-label="查看版本更新日志">
        <span class="app-version-code">版本</span>
        <span class="app-version-title">正在读取更新日志</span>
      </button>
    `;
    widget.querySelector('.app-version-trigger')?.addEventListener('click', openModal);
    shell.appendChild(widget);
    loadVersionInfo(widget);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mountWidget);
  } else {
    mountWidget();
  }
}());
